import argparse
from array import array
import json
import math
import os
import random
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone

try:
    import igraph
    import numpy as np

    LAYOUT_AVAILABLE = True
except ImportError:
    igraph = None
    np = None
    LAYOUT_AVAILABLE = False


GOLDEN_ANGLE = math.pi * (3 - math.sqrt(5))
MAX_CLUSTER_SIZE = 18000
MAX_CLUSTER_SUMMARIES = 80
DEFAULT_MAX_NODES = 10000
SNAPSHOT_SCHEMA_VERSION = 2
LAYOUT_SEED = 42
LAYOUT_TARGET_RADIUS = 2200.0
LAYOUT_MIN_FORCE_COMPONENT = 5
BASE_WORK_COLUMNS = [
    "id",
    "title",
    "authors",
    "year",
    "doi",
    "openalex_id",
    "source",
    "publisher",
    "type",
    "abstract",
    "keywords",
    "metadata_status",
    "download_status",
    "origin_key",
    "source_pdf",
    "source_work_id",
    "relationship_type",
    "primary_field",
    "primary_subfield",
    "primary_domain",
    "primary_topic",
]

# OpenAlex topic columns may be absent on databases that predate the
# backfill; main() drops any that PRAGMA reports as missing.
OPTIONAL_WORK_COLUMNS = {
    "primary_field",
    "primary_subfield",
    "primary_domain",
    "primary_topic",
}

COUNTRY_REGIONS = {
    "US": "North America",
    "CA": "North America",
    "MX": "North America",
    "GB": "Europe",
    "UK": "Europe",
    "DE": "Europe",
    "FR": "Europe",
    "IT": "Europe",
    "ES": "Europe",
    "NL": "Europe",
    "BE": "Europe",
    "CH": "Europe",
    "AT": "Europe",
    "SE": "Europe",
    "NO": "Europe",
    "DK": "Europe",
    "FI": "Europe",
    "IE": "Europe",
    "PL": "Europe",
    "CZ": "Europe",
    "PT": "Europe",
    "GR": "Europe",
    "CN": "East Asia",
    "JP": "East Asia",
    "KR": "East Asia",
    "TW": "East Asia",
    "IN": "South Asia",
    "PK": "South Asia",
    "BD": "South Asia",
    "AU": "Oceania",
    "NZ": "Oceania",
    "BR": "Latin America",
    "AR": "Latin America",
    "CL": "Latin America",
    "CO": "Latin America",
    "ZA": "Africa",
    "NG": "Africa",
    "KE": "Africa",
    "EG": "Africa",
    "IL": "Middle East",
    "TR": "Middle East",
    "IR": "Middle East",
}

AREA_KEYWORDS = [
    ("Institutional economics", ["institution", "transaction cost", "property rights", "governance", "commons", "ostrom"]),
    ("Economics and finance", ["economic", "economics", "market", "finance", "bank", "trade", "monetary", "fiscal", "capital", "industry", "firm"]),
    ("Anthropology and sociology", ["anthropolog", "sociolog", "culture", "cultural", "ethnograph", "ritual", "kinship", "society", "social"]),
    ("Law and political science", ["law", "legal", "court", "constitution", "politic", "policy", "state", "government", "regulation", "democracy"]),
    ("Environmental science", ["anthropocene", "climate", "ecolog", "biodiversity", "sustainab", "environment", "agricultur"]),
    ("History", ["history", "historical", "medieval", "century", "ancient", "colonial", "empire"]),
    ("Philosophy and science studies", ["philosoph", "epistem", "scientific", "science", "knowledge", "paradigm"]),
    ("Physics and complex systems", ["hurst", "fractal", "physics", "complex", "network", "statistical", "dynamics"]),
    ("Computer and data science", ["algorithm", "machine learning", "data", "software", "comput", "digital", "platform"]),
    ("Medicine and psychology", ["medical", "medicine", "health", "psycholog", "clinical", "patient", "neuro"]),
]

REGION_KEYWORDS = [
    ("Europe", ["europe", "european", "germany", "german", "france", "french", "britain", "british", "england", "italy", "spain", "dutch", "sweden", "poland"]),
    ("North America", ["united states", " u.s.", " usa", "america", "american", "canada", "canadian", "mexico"]),
    ("East Asia", ["china", "chinese", "japan", "japanese", "korea", "korean", "taiwan"]),
    ("South Asia", ["india", "indian", "pakistan", "bangladesh"]),
    ("Africa", ["africa", "african", "nigeria", "kenya", "ghana", "egypt", "south africa", "botswana"]),
    ("Latin America", ["latin america", "brazil", "brazilian", "argentina", "chile", "colombia", "bolivia"]),
    ("Middle East", ["middle east", "israel", "turkey", "iran", "ottoman"]),
    ("Oceania", ["australia", "australian", "new zealand"]),
]


def normalize_openalex_id(value):
    if not value:
        return None
    match = re.search(r"(W\d+)", str(value), re.IGNORECASE)
    return match.group(1).upper() if match else None


def normalize_doi(value):
    if not value:
        return None
    match = re.search(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", str(value), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return str(value).strip().upper() or None


def derive_node_id(row):
    return normalize_openalex_id(row.get("openalex_id")) or normalize_doi(row.get("doi")) or f"work:{row['id']}"


def work_status(row):
    metadata = str(row.get("metadata_status") or "pending").strip().lower()
    download = str(row.get("download_status") or "not_requested").strip().lower()
    if download == "downloaded":
        return "downloaded"
    if download in {"queued", "in_progress"}:
        return "queued_download"
    if download == "failed":
        return "failed_download"
    if metadata == "in_progress":
        return "enriching"
    if metadata == "failed":
        return "failed_enrichment"
    if metadata == "matched":
        return "matched"
    return "raw"


def status_allowed(filter_status, row):
    status = work_status(row)
    return filter_status == "all" or status == filter_status


def has_downloaded_metadata(row):
    if str(row.get("download_status") or "").strip().lower() != "downloaded":
        return False
    if str(row.get("metadata_status") or "").strip().lower() != "matched":
        return False
    return bool(str(row.get("title") or "").strip())


def source_label(row):
    source_pdf = str(row.get("source_pdf") or "").strip()
    if source_pdf:
        return source_pdf.rsplit("/", 1)[-1]
    origin = str(row.get("origin_key") or "").strip()
    if origin:
        return origin
    return "unknown"


def parse_json_field(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def stable_hash(value):
    text = str(value or "")
    result = 2166136261
    for char in text:
        result ^= ord(char)
        result = (result * 16777619) & 0xFFFFFFFF
    return result


def top_text(value, fallback="unknown"):
    value = re.sub(r"\s+", " ", str(value or "")).strip()
    return value if value else fallback


def year_decade(row):
    year = row.get("year")
    try:
        year = int(year)
    except (TypeError, ValueError):
        return "unknown year"
    return f"{(year // 10) * 10}s"


def openalex_payload(row):
    parsed = row.get("_openalex_json")
    if parsed is None and "_openalex_json" not in row:
        parsed = parse_json_field(row.get("openalex_json"))
        row["_openalex_json"] = parsed
    return parsed if isinstance(parsed, dict) else {}


UNKNOWN_FIELD = "Unknown field"


def work_field(row):
    """Academic field used for clustering.

    Prefers the OpenAlex `primary_field` backfilled into the works table; falls
    back to the inferred science area (then a placeholder) for works OpenAlex
    could not classify.
    """
    if "_work_field" in row:
        return row["_work_field"]
    field = top_text(row.get("primary_field"), "")
    if not field:
        area = science_area(row)
        field = area if area and not area.lower().startswith("unknown") else UNKNOWN_FIELD
    row["_work_field"] = field
    return field


def work_domain(row):
    return top_text(row.get("primary_domain"), "")


def science_area(row):
    if "_science_area" in row:
        return row["_science_area"]
    data = openalex_payload(row)
    primary_topic = data.get("primary_topic")
    if isinstance(primary_topic, dict):
        for key in ("display_name", "name"):
            if primary_topic.get(key):
                row["_science_area"] = top_text(primary_topic.get(key))
                return row["_science_area"]
        for parent_key in ("field", "subfield", "domain"):
            parent = primary_topic.get(parent_key)
            if isinstance(parent, dict) and parent.get("display_name"):
                row["_science_area"] = top_text(parent.get("display_name"))
                return row["_science_area"]

    topics = data.get("topics")
    if isinstance(topics, list):
        for topic in topics:
            if isinstance(topic, dict):
                if topic.get("display_name"):
                    row["_science_area"] = top_text(topic.get("display_name"))
                    return row["_science_area"]
                for parent_key in ("field", "subfield", "domain"):
                    parent = topic.get(parent_key)
                    if isinstance(parent, dict) and parent.get("display_name"):
                        row["_science_area"] = top_text(parent.get("display_name"))
                        return row["_science_area"]

    concepts = data.get("concepts")
    if isinstance(concepts, list):
        ranked = sorted(
            (concept for concept in concepts if isinstance(concept, dict) and concept.get("display_name")),
            key=lambda concept: (-float(concept.get("score") or 0), int(concept.get("level") or 9), concept.get("display_name") or ""),
        )
        if ranked:
            row["_science_area"] = top_text(ranked[0].get("display_name"))
            return row["_science_area"]

    keywords = data.get("keywords")
    if isinstance(keywords, list):
        for keyword in keywords:
            if isinstance(keyword, dict) and keyword.get("keyword"):
                row["_science_area"] = top_text(keyword.get("keyword"))
                return row["_science_area"]
            if isinstance(keyword, str) and keyword.strip():
                row["_science_area"] = top_text(keyword)
                return row["_science_area"]

    raw_keywords = str(row.get("keywords") or "").strip()
    if raw_keywords:
        first = re.split(r"[,;|]", raw_keywords)[0]
        if first.strip():
            row["_science_area"] = top_text(first)
            return row["_science_area"]

    inferred = infer_area_from_text(row)
    if inferred:
        row["_science_area"] = inferred
        return row["_science_area"]

    source = source_label(row)
    if not is_operational_source(source):
        cleaned = clean_cluster_label(source)
        cleaned = re.sub(r"[_-]?refs[_-]?(physical)?[_-]?p\d+", "", cleaned, flags=re.IGNORECASE)
        row["_science_area"] = top_text(cleaned, "unknown area")
        return row["_science_area"]

    work_type = str(row.get("type") or "").strip()
    row["_science_area"] = top_text(work_type, "unknown area")
    return row["_science_area"]


def work_countries(row):
    if "_work_countries" in row:
        return row["_work_countries"]
    data = openalex_payload(row)
    countries = []
    for authorship in data.get("authorships") or []:
        if not isinstance(authorship, dict):
            continue
        for country in authorship.get("countries") or []:
            country = str(country or "").strip().upper()
            if country and country not in countries:
                countries.append(country)
        for institution in authorship.get("institutions") or []:
            if not isinstance(institution, dict):
                continue
            country = str(institution.get("country_code") or "").strip().upper()
            if country and country not in countries:
                countries.append(country)
    row["_work_countries"] = countries
    return row["_work_countries"]


def work_region(row):
    if "_work_region" in row:
        return row["_work_region"]
    countries = work_countries(row)
    if not countries:
        row["_work_region"] = infer_region_from_text(row) or "unknown region"
        return row["_work_region"]
    region_counts = Counter(COUNTRY_REGIONS.get(country, "Other region") for country in countries)
    row["_work_region"] = region_counts.most_common(1)[0][0]
    return row["_work_region"]


def clean_cluster_label(value):
    value = re.sub(r"\s+", " ", str(value or "")).strip()
    value = re.sub(r"\.(pdf|json|bib)$", "", value, flags=re.IGNORECASE)
    return value[:96] if value else "Unknown source"


def is_operational_source(value):
    value = str(value or "").strip().lower()
    return (
        not value
        or value == "unknown"
        or re.match(r"^(downloaded|failed|queued|raw|matched)_", value)
        or re.match(r"^[a-z_]+:\d+$", value)
    )


def infer_area_from_text(row):
    text = " ".join(
        str(row.get(key) or "")
        for key in ("title", "abstract", "keywords", "source", "publisher", "source_pdf", "origin_key")
    ).lower()
    if not text.strip():
        return None
    scores = []
    for label, terms in AREA_KEYWORDS:
        score = sum(text.count(term) for term in terms)
        if score:
            scores.append((score, label))
    if not scores:
        return None
    return sorted(scores, key=lambda item: (-item[0], item[1]))[0][1]


def infer_region_from_text(row):
    text = " ".join(
        str(row.get(key) or "")
        for key in ("title", "abstract", "keywords", "source", "publisher", "source_pdf", "origin_key")
    ).lower()
    if not text.strip():
        return None
    scores = []
    for label, terms in REGION_KEYWORDS:
        score = sum(text.count(term) for term in terms)
        if score:
            scores.append((score, label))
    if not scores:
        return None
    return sorted(scores, key=lambda item: (-item[0], item[1]))[0][1]


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.size = {}

    def add(self, item):
        if item not in self.parent:
            self.parent[item] = item
            self.size[item] = 1

    def find(self, item):
        self.add(item)
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]


def fibonacci_direction(index, total):
    y = 1 - 2 * (index + 0.5) / max(1, total)
    ring = math.sqrt(max(0.0, 1 - y * y))
    theta = index * GOLDEN_ANGLE
    return (math.cos(theta) * ring, y, math.sin(theta) * ring)


def layout_component(size, local_edges):
    if size <= 1:
        return [(0.0, 0.0, 0.0)]
    if size < LAYOUT_MIN_FORCE_COMPONENT or not local_edges:
        radius = 14.0 + math.pow(size, 0.5) * 6.0
        return [
            tuple(value * radius * (0.4 + 0.6 * (index + 1) / size) for value in fibonacci_direction(index, size))
            for index in range(size)
        ]
    graph = igraph.Graph(size, local_edges)
    niter = int(min(200, 90 + math.sqrt(size) * 1.5))
    layout = graph.layout_fruchterman_reingold(dim=3, niter=niter)
    return [tuple(coord) for coord in layout]


def normalize_component(coords, size):
    points = np.asarray(coords, dtype=np.float64)
    points -= points.mean(axis=0)
    if size > 1:
        spread = float(np.percentile(np.linalg.norm(points, axis=1), 90))
        target = math.pow(size, 0.4) * 40.0
        if spread > 1e-9:
            points *= target / spread
    return points


def compute_field_layout(clusters, edges):
    """3D layout that groups works into academic-field territories.

    Each field cluster is laid out on its own — Fruchterman-Reingold 3D over its
    internal citation edges when it has enough of them, otherwise a compact
    Fibonacci ball — then packed onto golden-angle shells with the largest field
    at the origin. Cross-field citations are not used for positioning (they are
    rendered as bridge links), so spatial grouping reflects fields.

    Returns {node_id: (x, y, z)} or None when igraph/numpy are unavailable.
    """
    if not LAYOUT_AVAILABLE or not clusters:
        return None
    igraph.set_random_number_generator(random.Random(LAYOUT_SEED))

    cluster_of = {}
    local_index = {}
    for cluster_id, (_label, group) in enumerate(clusters):
        for position, node_id in enumerate(group):
            cluster_of[node_id] = cluster_id
            local_index[node_id] = position

    cluster_edges = defaultdict(set)
    for edge in edges:
        source, target = edge["s"], edge["t"]
        if source == target:
            continue
        cluster_source = cluster_of.get(source)
        if cluster_source is None or cluster_source != cluster_of.get(target):
            continue  # cross-field edges do not drive positioning
        pair = (local_index[source], local_index[target])
        cluster_edges[cluster_source].add((min(pair), max(pair)))

    layouts = []
    radii = []
    for cluster_id, (_label, group) in enumerate(clusters):
        points = normalize_component(
            layout_component(len(group), sorted(cluster_edges.get(cluster_id, ()))),
            len(group),
        )
        layouts.append(points)
        radii.append(float(np.linalg.norm(points, axis=1).max()) if len(points) > 1 else 8.0)

    # Largest field at the origin, the rest on golden-angle shells around it.
    order = sorted(range(len(clusters)), key=lambda cluster_id: -len(clusters[cluster_id][1]))
    giant_radius = radii[order[0]]
    satellite_count = max(1, len(order) - 1)
    positions = {}
    for rank, cluster_id in enumerate(order):
        if rank == 0:
            offset = np.zeros(3)
        else:
            direction = np.asarray(fibonacci_direction(rank - 1, satellite_count))
            distance = giant_radius + radii[cluster_id] + 160.0 + 70.0 * math.pow(rank, 0.55)
            offset = direction * distance
        placed = layouts[cluster_id] + offset
        for position, node_id in enumerate(clusters[cluster_id][1]):
            point = placed[position]
            positions[node_id] = (float(point[0]), float(point[1]), float(point[2]))

    max_radius = max((math.sqrt(x * x + y * y + z * z) for x, y, z in positions.values()), default=0.0)
    if max_radius > 1e-9:
        scale = min(LAYOUT_TARGET_RADIUS / max_radius, 8.0)
        positions = {
            node_id: (x * scale, y * scale, z * scale)
            for node_id, (x, y, z) in positions.items()
        }
    return positions


def split_field_cluster(field, members, node_by_id, degree):
    """Keep a field as one cluster, splitting only oversized fields.

    Large fields are first broken out by OpenAlex subfield, then chunked by size
    if a subfield is still too large, so territories stay legible.
    """
    if len(members) <= MAX_CLUSTER_SIZE:
        return [(field, members)]
    by_subfield = defaultdict(list)
    for node_id in members:
        subfield = top_text(node_by_id[node_id].get("primary_subfield"), "")
        by_subfield[subfield].append(node_id)
    if len(by_subfield) <= 1:
        return split_large_cluster(field, members, node_by_id, degree)
    parts = []
    for subfield, sub_members in by_subfield.items():
        label = field if not subfield else f"{field} - {subfield}"
        parts.extend(split_large_cluster(label, sub_members, node_by_id, degree))
    return parts


def cluster_geometry_from_positions(group, positions):
    points = np.asarray([positions[node_id] for node_id in group], dtype=np.float64)
    center = points.mean(axis=0)
    if len(group) > 1:
        radius = float(np.percentile(np.linalg.norm(points - center, axis=1), 88))
    else:
        radius = 24.0
    return (float(center[0]), float(center[1]), float(center[2])), max(24.0, radius)


def cluster_center(index, size, total_clusters, label):
    seed = stable_hash(label)
    rank = index + 1
    size_scale = math.log1p(max(1, size))
    radius = 120.0 + math.pow(rank, 0.62) * 96.0 + size_scale * 34.0
    theta = index * GOLDEN_ANGLE + ((seed % 997) / 997.0 - 0.5) * 0.74
    y_band = ((seed >> 10) % 2001) / 1000.0 - 1.0
    y = y_band * (150.0 + math.pow(rank, 0.48) * 28.0)
    if index == 0:
        radius *= 0.28
        y *= 0.2
    elif index < 12:
        radius *= 0.58 + index * 0.045
    wobble = 0.74 + ((seed >> 21) % 1000) / 1000.0 * 0.62
    scale = 1.0 + min(2.1, size_scale * 0.18)
    return (
        math.cos(theta) * radius * scale * wobble,
        y,
        math.sin(theta) * radius * scale / max(0.55, wobble),
    )


def local_position(index, count, degree, cluster_size, label):
    if count <= 1:
        return (0.0, 0.0, 0.0)
    y = 1 - (2 * (index + 0.5) / count)
    ring = math.sqrt(max(0.0, 1 - y * y))
    theta = index * GOLDEN_ANGLE + (stable_hash(label) % 360) * math.pi / 180
    size_radius = math.pow(max(1, cluster_size), 0.42)
    base_radius = 24.0 + min(720.0, 7.4 * size_radius)
    if str(label).startswith("low-signal:"):
        base_radius *= 0.82
    degree_pull = 1.0 / (1.0 + math.log1p(max(0, degree)) * 0.11)
    jitter = 0.86 + ((stable_hash(f"{label}:{index}") % 1000) / 1000.0) * 0.24
    radius = base_radius * degree_pull * jitter
    return (
        math.cos(theta) * ring * radius,
        y * radius * (0.58 + (stable_hash(label) % 300) / 1000.0),
        math.sin(theta) * ring * radius,
    )


def split_large_cluster(label, group, node_by_id, degree):
    if len(group) <= MAX_CLUSTER_SIZE:
        return [(label, group)]
    chunks = []
    group.sort(key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))
    for offset in range(0, len(group), MAX_CLUSTER_SIZE):
        chunks.append((f"{label}:part{offset // MAX_CLUSTER_SIZE + 1}", group[offset : offset + MAX_CLUSTER_SIZE]))
    return chunks


def top_counter_items(counter, limit=5):
    ranked = sorted(counter.items(), key=lambda item: (str(item[0]).startswith("unknown "), -item[1], str(item[0])))
    return [{"label": label, "count": int(count)} for label, count in ranked[:limit]]


def dominant_meaningful(counter, unknown_label):
    if not counter:
        return unknown_label
    for label, _count in counter.most_common():
        if str(label or "").strip().lower() != unknown_label:
            return label
    return counter.most_common(1)[0][0]


def cluster_visual_radius(size, label):
    radius = 24.0 + min(720.0, 7.4 * math.pow(max(1, size), 0.42))
    if str(label).startswith("low-signal:"):
        radius *= 0.82
    return radius


def build_cluster_summary(cluster_id, label, group, node_by_id, degree, center, radius=None, kind="field"):
    top_nodes = sorted(group, key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))[:5]
    decade_counts = Counter(year_decade(node_by_id[node_id]) for node_id in group)
    # Within a field territory the sub-structure of interest is the subfield;
    # fall back to the inferred science area for works without OpenAlex data.
    subfield_counts = Counter(
        top_text(node_by_id[node_id].get("primary_subfield"), "") or science_area(node_by_id[node_id])
        for node_id in group
    )
    region_counts = Counter(work_region(node_by_id[node_id]) for node_id in group)
    country_counts = Counter(country for node_id in group for country in work_countries(node_by_id[node_id]))
    type_counts = Counter(top_text(node_by_id[node_id].get("type"), "unknown type") for node_id in group)
    years = []
    for node_id in group:
        try:
            year = int(node_by_id[node_id].get("year"))
            years.append(year)
        except (TypeError, ValueError):
            pass
    field = label.split(" - ", 1)[0]
    dominant_decade = decade_counts.most_common(1)[0][0] if decade_counts else "unknown year"
    dominant_region = dominant_meaningful(region_counts, "unknown region")
    dominant_country = country_counts.most_common(1)[0][0] if country_counts else ""
    dominant_domain = dominant_meaningful(
        Counter(work_domain(node_by_id[node_id]) for node_id in group), ""
    )
    return {
        "id": int(cluster_id),
        "label": clean_cluster_label(label),
        "kind": kind,
        "field": field,
        "domain": dominant_domain or None,
        "size": int(len(group)),
        "degree": int(sum(degree[node_id] for node_id in group)),
        "area": field,
        "region": dominant_region,
        "country": dominant_country,
        "decade": dominant_decade,
        "year_min": min(years) if years else None,
        "year_max": max(years) if years else None,
        "radius": round(radius if radius is not None else cluster_visual_radius(len(group), label), 3),
        "x": round(center[0], 3),
        "y": round(center[1], 3),
        "z": round(center[2], 3),
        "areas": top_counter_items(subfield_counts),
        "regions": top_counter_items(region_counts),
        "countries": top_counter_items(country_counts),
        "types": top_counter_items(type_counts),
        "top_titles": [node_by_id[node_id].get("title") or "Untitled" for node_id in top_nodes],
    }


def fetch_openalex_json_for_kept_rows(conn, node_by_id):
    rows_by_work_id = {int(row["id"]): row for row in node_by_id.values() if row.get("id") is not None}
    work_ids = list(rows_by_work_id.keys())
    if not work_ids:
        return
    for offset in range(0, len(work_ids), 900):
        chunk = work_ids[offset : offset + 900]
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(f"SELECT id, openalex_json FROM works WHERE id IN ({placeholders})", chunk):
            item = rows_by_work_id.get(int(row["id"]))
            if not item:
                continue
            item["openalex_json"] = row["openalex_json"]
            item.pop("_openalex_json", None)


def write_snapshot(payload, snapshot_dir):
    os.makedirs(snapshot_dir, exist_ok=True)

    nodes = payload.get("nodes") or []
    edges = payload.get("edges") or []
    node_index = {node.get("id"): index for index, node in enumerate(nodes)}

    positions = array("f")
    nodes_meta = []
    for node in nodes:
        positions.extend(
            [
                float(node.get("x") or 0.0),
                float(node.get("y") or 0.0),
                float(node.get("z") or 0.0),
            ]
        )
        nodes_meta.append({key: value for key, value in node.items() if key not in {"x", "y", "z"}})

    edge_triplets = array("I")
    encoded_edge_count = 0
    for edge in edges:
        source_index = node_index.get(edge.get("s"))
        target_index = node_index.get(edge.get("t"))
        if source_index is None or target_index is None:
            continue
        edge_triplets.extend([source_index, target_index, 1 if edge.get("r") == "cited_by" else 0])
        encoded_edge_count += 1

    with open(os.path.join(snapshot_dir, "nodes.bin"), "wb") as handle:
        positions.tofile(handle)
    with open(os.path.join(snapshot_dir, "edges.bin"), "wb") as handle:
        edge_triplets.tofile(handle)
    with open(os.path.join(snapshot_dir, "nodes_meta.json"), "w", encoding="utf-8") as handle:
        json.dump(nodes_meta, handle, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(snapshot_dir, "clusters.json"), "w", encoding="utf-8") as handle:
        json.dump(payload.get("clusters") or [], handle, ensure_ascii=False, separators=(",", ":"))

    manifest = {
        "source": "snapshot",
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layout": payload.get("layout") or {"algorithm": "synthetic-spiral"},
        "stats": payload.get("stats") or {},
        "files": {
            "nodes": "nodes.bin",
            "edges": "edges.bin",
            "nodes_meta": "nodes_meta.json",
            "clusters": "clusters.json",
        },
        "buffers": {
            "nodes": {"count": len(nodes), "array": "float32", "stride": 3, "fields": ["x", "y", "z"]},
            "edges": {
                "count": encoded_edge_count,
                "array": "uint32",
                "stride": 3,
                "fields": ["source_index", "target_index", "relationship_code"],
                "relationship_codes": {"references": 0, "cited_by": 1},
            },
        },
        "clusters": payload.get("clusters") or [],
    }
    with open(os.path.join(snapshot_dir, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, separators=(",", ":"))
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--max-nodes", type=int, default=DEFAULT_MAX_NODES)
    parser.add_argument("--relationship", choices=["references", "cited_by", "both"], default="both")
    parser.add_argument(
        "--status",
        choices=["downloaded", "queued_download", "enriching", "matched", "failed_enrichment", "failed_download", "raw", "all"],
        default="all",
    )
    parser.add_argument("--year-from", type=int, default=None)
    parser.add_argument("--year-to", type=int, default=None)
    parser.add_argument("--corpus-id", type=int, default=None)
    parser.add_argument("--snapshot-dir", default=None)
    parser.add_argument("--require-downloaded-metadata", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    available_columns = {row[1] for row in conn.execute("PRAGMA table_info(works)")}
    query_columns = [
        column
        for column in BASE_WORK_COLUMNS
        if column not in OPTIONAL_WORK_COLUMNS or column in available_columns
    ]
    selected_columns = ", ".join(f"w.{column}" for column in query_columns)
    sql = f"SELECT {selected_columns} FROM works w"
    params = []
    conditions = []
    if args.corpus_id is not None:
        sql += " JOIN corpus_works cw ON cw.work_id = w.id"
        conditions.append("cw.corpus_id = ?")
        params.append(args.corpus_id)
    if args.year_from is not None:
        conditions.append("(w.year IS NOT NULL AND w.year >= ?)")
        params.append(args.year_from)
    if args.year_to is not None:
        conditions.append("(w.year IS NOT NULL AND w.year <= ?)")
        params.append(args.year_to)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    rows = [row for row in rows if status_allowed(args.status, row)]
    if args.require_downloaded_metadata:
        rows = [row for row in rows if has_downloaded_metadata(row)]

    node_by_id = {}
    work_id_to_node_id = {}
    for row in rows:
        node_id = derive_node_id(row)
        work_id_to_node_id[row["id"]] = node_id
        if node_id in node_by_id:
            continue
        node_by_id[node_id] = row

    allowed = set(node_by_id.keys())
    edges = []
    edge_set = set()
    uf = UnionFind()
    degree = defaultdict(int)
    relationship_counts = {"references": 0, "cited_by": 0}

    for node_id in allowed:
        uf.add(node_id)

    def add_edge(source, target, relationship):
        if not source or not target or source == target:
            return
        if source not in allowed or target not in allowed:
            return
        rel = relationship if relationship in {"references", "cited_by"} else "references"
        if args.relationship != "both" and rel != args.relationship:
            return
        key = (source, target, rel)
        if key in edge_set:
            return
        edge_set.add(key)
        edges.append({"s": source, "t": target, "r": rel})
        degree[source] += 1
        degree[target] += 1
        relationship_counts[rel] += 1
        uf.union(source, target)

    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='citation_edges'").fetchone():
        for row in conn.execute("SELECT source_id, target_id, relationship_type FROM citation_edges"):
            add_edge(row["source_id"], row["target_id"], row["relationship_type"])

    for row in rows:
        source_work_id = row.get("source_work_id")
        if source_work_id is None:
            continue
        source_id = work_id_to_node_id.get(source_work_id)
        source_id = source_id or normalize_openalex_id(source_work_id) or normalize_doi(source_work_id)
        add_edge(source_id, work_id_to_node_id.get(row["id"]), row.get("relationship_type") or "references")

    if args.max_nodes > 0 and len(node_by_id) > args.max_nodes:
        keep = set(
            sorted(
                node_by_id.keys(),
                key=lambda node_id: (
                    -degree[node_id],
                    0 if work_status(node_by_id[node_id]) == "downloaded" else 1,
                    str(node_by_id[node_id].get("title") or ""),
                    node_id,
                ),
            )[: args.max_nodes]
        )
        node_by_id = {node_id: row for node_id, row in node_by_id.items() if node_id in keep}
        allowed = keep
        edges = [edge for edge in edges if edge["s"] in allowed and edge["t"] in allowed]
        degree = defaultdict(int)
        relationship_counts = {"references": 0, "cited_by": 0}
        uf = UnionFind()
        for node_id in allowed:
            uf.add(node_id)
        for edge in edges:
            degree[edge["s"]] += 1
            degree[edge["t"]] += 1
            relationship_counts[edge["r"]] += 1
            uf.union(edge["s"], edge["t"])

    fetch_openalex_json_for_kept_rows(conn, node_by_id)

    groups = defaultdict(list)
    for node_id in node_by_id:
        groups[uf.find(node_id)].append(node_id)
    components = sorted(groups.values(), key=lambda group: (-len(group), group[0]))
    component_index = {}
    component_sizes = {}
    for index, group in enumerate(components):
        for node_id in group:
            component_index[node_id] = index
            component_sizes[node_id] = len(group)

    # Cluster by academic field (OpenAlex primary_field, backfilled into the
    # works table). The graph then forms field territories rather than citation
    # communities; oversized fields are split by subfield to stay legible.
    field_groups = defaultdict(list)
    for node_id, row in node_by_id.items():
        field_groups[work_field(row)].append(node_id)

    cluster_parts = []
    for field, members in field_groups.items():
        cluster_parts.extend(split_field_cluster(field, members, node_by_id, degree))

    clusters = sorted(cluster_parts, key=lambda item: (-len(item[1]), item[0]))
    cluster_index = {}
    cluster_sizes = {}
    for index, (_label, group) in enumerate(clusters):
        for node_id in group:
            cluster_index[node_id] = index
            cluster_sizes[node_id] = len(group)

    layout_positions = compute_field_layout(clusters, edges)
    layout_info = {
        "algorithm": "igraph-fr3d-field" if layout_positions else "synthetic-spiral",
        "seed": LAYOUT_SEED,
    }

    total_clusters = len(clusters)
    cluster_summaries = []
    nodes = []
    for index, (label, group) in enumerate(clusters):
        field_root = label.split(" - ", 1)[0]
        cluster_kind = "unknown_field" if field_root == UNKNOWN_FIELD else "field"
        if layout_positions:
            center, radius = cluster_geometry_from_positions(group, layout_positions)
        else:
            center = cluster_center(index, len(group), total_clusters, label)
            radius = None
        if len(cluster_summaries) < MAX_CLUSTER_SUMMARIES:
            cluster_summaries.append(
                build_cluster_summary(index, label, group, node_by_id, degree, center, radius, kind=cluster_kind)
            )
        group.sort(key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))
        for local_index, node_id in enumerate(group):
            row = node_by_id[node_id]
            if layout_positions:
                px, py, pz = layout_positions[node_id]
            else:
                lx, ly, lz = local_position(local_index, len(group), degree[node_id], len(group), label)
                px, py, pz = center[0] + lx, center[1] + ly, center[2] + lz
            nodes.append(
                {
                    "id": node_id,
                    "work_id": row.get("id"),
                    "title": row.get("title") or "Untitled",
                    "authors": re.sub(r"\s+", " ", str(row.get("authors") or "")).strip()[:120],
                    "year": row.get("year"),
                    "type": row.get("type") or "",
                    "status": work_status(row),
                    "source": source_label(row),
                    "field": work_field(row),
                    "area": science_area(row),
                    "region": work_region(row),
                    "countries": work_countries(row)[:4],
                    "degree": int(degree[node_id]),
                    "component": int(component_index[node_id]),
                    "component_size": int(component_sizes[node_id]),
                    "cluster": int(cluster_index[node_id]),
                    "cluster_size": int(cluster_sizes[node_id]),
                    "cluster_kind": cluster_kind,
                    "x": round(px, 3),
                    "y": round(py, 3),
                    "z": round(pz, 3),
                }
            )

    area_counts = Counter(node.get("area") or "unknown area" for node in nodes)
    region_counts = Counter(node.get("region") or "unknown region" for node in nodes)
    country_counts = Counter(country for node in nodes for country in (node.get("countries") or []))
    known_area_count = sum(count for label, count in area_counts.items() if label != "unknown area")
    known_region_count = sum(count for label, count in region_counts.items() if label != "unknown region")

    payload = {
        "source": "api",
        "layout": layout_info,
        "nodes": nodes,
        "edges": edges,
        "clusters": cluster_summaries,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "component_count": len(components),
            "cluster_count": len(clusters),
            "relationship_counts": relationship_counts,
            "total_work_count": len(rows),
            "shown_node_count": len(nodes),
            "max_nodes": args.max_nodes,
            "is_limited": bool(args.max_nodes > 0 and len(rows) > len(nodes)),
            "area_coverage": {"known": int(known_area_count), "total": len(nodes)},
            "region_coverage": {"known": int(known_region_count), "total": len(nodes)},
            "top_areas": top_counter_items(area_counts),
            "top_regions": top_counter_items(region_counts),
            "top_countries": top_counter_items(country_counts),
        },
    }
    conn.close()
    if args.snapshot_dir:
        payload = write_snapshot(payload, args.snapshot_dir)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
