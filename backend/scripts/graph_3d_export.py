import argparse
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict


GOLDEN_ANGLE = math.pi * (3 - math.sqrt(5))
MAX_CLUSTER_SIZE = 18000
MAX_CLUSTER_SUMMARIES = 80
MIN_STANDALONE_CLUSTER_SIZE = 8

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


def label_part(value, limit):
    return top_text(value).replace(":", " - ")[:limit]


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


def science_area(row):
    data = openalex_payload(row)
    primary_topic = data.get("primary_topic")
    if isinstance(primary_topic, dict):
        for key in ("display_name", "name"):
            if primary_topic.get(key):
                return top_text(primary_topic.get(key))
        for parent_key in ("field", "subfield", "domain"):
            parent = primary_topic.get(parent_key)
            if isinstance(parent, dict) and parent.get("display_name"):
                return top_text(parent.get("display_name"))

    topics = data.get("topics")
    if isinstance(topics, list):
        for topic in topics:
            if isinstance(topic, dict):
                if topic.get("display_name"):
                    return top_text(topic.get("display_name"))
                for parent_key in ("field", "subfield", "domain"):
                    parent = topic.get(parent_key)
                    if isinstance(parent, dict) and parent.get("display_name"):
                        return top_text(parent.get("display_name"))

    concepts = data.get("concepts")
    if isinstance(concepts, list):
        ranked = sorted(
            (concept for concept in concepts if isinstance(concept, dict) and concept.get("display_name")),
            key=lambda concept: (-float(concept.get("score") or 0), int(concept.get("level") or 9), concept.get("display_name") or ""),
        )
        if ranked:
            return top_text(ranked[0].get("display_name"))

    keywords = data.get("keywords")
    if isinstance(keywords, list):
        for keyword in keywords:
            if isinstance(keyword, dict) and keyword.get("keyword"):
                return top_text(keyword.get("keyword"))
            if isinstance(keyword, str) and keyword.strip():
                return top_text(keyword)

    raw_keywords = str(row.get("keywords") or "").strip()
    if raw_keywords:
        first = re.split(r"[,;|]", raw_keywords)[0]
        if first.strip():
            return top_text(first)

    inferred = infer_area_from_text(row)
    if inferred:
        return inferred

    source = source_label(row)
    if not is_operational_source(source):
        cleaned = clean_cluster_label(source)
        cleaned = re.sub(r"[_-]?refs[_-]?(physical)?[_-]?p\d+", "", cleaned, flags=re.IGNORECASE)
        return top_text(cleaned, "unknown area")

    work_type = str(row.get("type") or "").strip()
    return top_text(work_type, "unknown area")


def work_countries(row):
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
    return countries


def work_region(row):
    countries = work_countries(row)
    if not countries:
        return infer_region_from_text(row) or "unknown region"
    region_counts = Counter(COUNTRY_REGIONS.get(country, "Other region") for country in countries)
    return region_counts.most_common(1)[0][0]


def cluster_seed_label(row):
    decade = year_decade(row)
    area = science_area(row)
    region = work_region(row)
    source = source_label(row)
    if source != "unknown":
        return f"source:{label_part(source, 120)}:{label_part(area, 80)}:{label_part(region, 40)}:{decade}"
    return f"topic:{label_part(area, 100)}:{label_part(region, 40)}:{decade}"


def low_signal_cluster_label(row):
    decade = year_decade(row)
    area = science_area(row)
    region = work_region(row)
    return f"low-signal:{label_part(area, 100)}:{label_part(region, 40)}:{decade}"


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


def build_adjacency(edges, allowed):
    adjacency = {node_id: set() for node_id in allowed}
    for edge in edges:
        source = edge["s"]
        target = edge["t"]
        if source in adjacency and target in adjacency:
            adjacency[source].add(target)
            adjacency[target].add(source)
    return adjacency


def propagate_cluster_labels(node_by_id, adjacency, degree, iterations=8):
    labels = {node_id: cluster_seed_label(row) for node_id, row in node_by_id.items()}
    ordered_nodes = sorted(
        node_by_id.keys(),
        key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id),
    )
    for _ in range(iterations):
        changed = 0
        next_labels = labels.copy()
        for node_id in ordered_nodes:
            neighbours = adjacency.get(node_id) or ()
            if not neighbours:
                continue
            scores = defaultdict(float)
            scores[labels[node_id]] += 0.35
            for neighbour_id in neighbours:
                scores[labels[neighbour_id]] += 1.0 + min(2.5, math.log1p(degree[neighbour_id]) * 0.3)
            best_label = min(scores.items(), key=lambda item: (-item[1], item[0]))[0]
            if best_label != labels[node_id]:
                next_labels[node_id] = best_label
                changed += 1
        labels = next_labels
        if changed == 0:
            break
    return labels


def split_large_cluster(label, group, node_by_id, degree):
    if len(group) <= MAX_CLUSTER_SIZE:
        return [(label, group)]
    chunks = []
    group.sort(key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))
    for offset in range(0, len(group), MAX_CLUSTER_SIZE):
        chunks.append((f"{label}:part{offset // MAX_CLUSTER_SIZE + 1}", group[offset : offset + MAX_CLUSTER_SIZE]))
    return chunks


def top_counter_items(counter, limit=5):
    return [{"label": label, "count": int(count)} for label, count in counter.most_common(limit)]


def cluster_visual_radius(size, label):
    radius = 24.0 + min(720.0, 7.4 * math.pow(max(1, size), 0.42))
    if str(label).startswith("low-signal:"):
        radius *= 0.82
    return radius


def build_cluster_summary(cluster_id, label, group, node_by_id, degree, center):
    top_nodes = sorted(group, key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))[:5]
    source_counts = Counter(source_label(node_by_id[node_id]) for node_id in group)
    decade_counts = Counter(year_decade(node_by_id[node_id]) for node_id in group)
    area_counts = Counter(science_area(node_by_id[node_id]) for node_id in group)
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
    dominant_source = source_counts.most_common(1)[0][0] if source_counts else "unknown"
    dominant_decade = decade_counts.most_common(1)[0][0] if decade_counts else "unknown year"
    dominant_area = area_counts.most_common(1)[0][0] if area_counts else "unknown area"
    dominant_region = region_counts.most_common(1)[0][0] if region_counts else "unknown region"
    dominant_country = country_counts.most_common(1)[0][0] if country_counts else ""
    title = node_by_id[top_nodes[0]].get("title") if top_nodes else ""
    if str(label).startswith("low-signal:"):
        parts = str(label)[len("low-signal:") :].split(":")
        area = parts[0] if parts else dominant_area
        region = parts[1] if len(parts) > 1 else dominant_region
        decade = parts[2] if len(parts) > 2 else dominant_decade
        part = f" {parts[3]}" if len(parts) > 3 else ""
        display_label = f"Low-link {clean_cluster_label(area)}, {region}, {decade}{part}"
    elif dominant_source and dominant_source != "unknown" and clean_cluster_label(dominant_source) != clean_cluster_label(dominant_area):
        display_label = f"{clean_cluster_label(dominant_area)} / {clean_cluster_label(dominant_source)}"
    elif title:
        display_label = f"{clean_cluster_label(dominant_area)} / {clean_cluster_label(title)}"
    else:
        display_label = clean_cluster_label(label)
    return {
        "id": int(cluster_id),
        "label": display_label,
        "kind": "low_signal" if str(label).startswith("low-signal:") else "community",
        "size": int(len(group)),
        "degree": int(sum(degree[node_id] for node_id in group)),
        "source": dominant_source,
        "area": dominant_area,
        "region": dominant_region,
        "country": dominant_country,
        "decade": dominant_decade,
        "year_min": min(years) if years else None,
        "year_max": max(years) if years else None,
        "radius": round(cluster_visual_radius(len(group), label), 3),
        "x": round(center[0], 3),
        "y": round(center[1], 3),
        "z": round(center[2], 3),
        "areas": top_counter_items(area_counts),
        "regions": top_counter_items(region_counts),
        "countries": top_counter_items(country_counts),
        "types": top_counter_items(type_counts),
        "top_titles": [node_by_id[node_id].get("title") or "Untitled" for node_id in top_nodes],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--max-nodes", type=int, default=120000)
    parser.add_argument("--relationship", choices=["references", "cited_by", "both"], default="both")
    parser.add_argument(
        "--status",
        choices=["downloaded", "queued_download", "enriching", "matched", "failed_enrichment", "failed_download", "raw", "all"],
        default="all",
    )
    parser.add_argument("--year-from", type=int, default=None)
    parser.add_argument("--year-to", type=int, default=None)
    parser.add_argument("--corpus-id", type=int, default=None)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    sql = "SELECT w.* FROM works w"
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
                key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id),
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

    adjacency = build_adjacency(edges, allowed)
    cluster_labels = propagate_cluster_labels(node_by_id, adjacency, degree)
    raw_clusters = defaultdict(list)
    for node_id, label in cluster_labels.items():
        raw_clusters[label].append(node_id)

    merged_low_signal_clusters = defaultdict(list)
    retained_raw_clusters = []
    for label, group in raw_clusters.items():
        if len(group) < MIN_STANDALONE_CLUSTER_SIZE:
            for node_id in group:
                merged_low_signal_clusters[low_signal_cluster_label(node_by_id[node_id])].append(node_id)
        else:
            retained_raw_clusters.append((label, group))
    retained_raw_clusters.extend(merged_low_signal_clusters.items())

    cluster_parts = []
    for label, group in retained_raw_clusters:
        cluster_parts.extend(split_large_cluster(label, group, node_by_id, degree))
    clusters = sorted(cluster_parts, key=lambda item: (-len(item[1]), item[0]))
    cluster_index = {}
    cluster_sizes = {}
    for index, (_label, group) in enumerate(clusters):
        for node_id in group:
            cluster_index[node_id] = index
            cluster_sizes[node_id] = len(group)

    total_clusters = len(clusters)
    cluster_summaries = []
    nodes = []
    for index, (label, group) in enumerate(clusters):
        center = cluster_center(index, len(group), total_clusters, label)
        if len(cluster_summaries) < MAX_CLUSTER_SUMMARIES:
            cluster_summaries.append(build_cluster_summary(index, label, group, node_by_id, degree, center))
        group.sort(key=lambda node_id: (-degree[node_id], str(node_by_id[node_id].get("title") or ""), node_id))
        for local_index, node_id in enumerate(group):
            row = node_by_id[node_id]
            lx, ly, lz = local_position(local_index, len(group), degree[node_id], len(group), label)
            nodes.append(
                {
                    "id": node_id,
                    "work_id": row.get("id"),
                    "title": row.get("title") or "Untitled",
                    "year": row.get("year"),
                    "type": row.get("type") or "",
                    "status": work_status(row),
                    "source": source_label(row),
                    "area": science_area(row),
                    "region": work_region(row),
                    "countries": work_countries(row)[:4],
                    "degree": int(degree[node_id]),
                    "component": int(component_index[node_id]),
                    "component_size": int(component_sizes[node_id]),
                    "cluster": int(cluster_index[node_id]),
                    "cluster_size": int(cluster_sizes[node_id]),
                    "cluster_kind": "low_signal" if str(label).startswith("low-signal:") else "community",
                    "x": round(center[0] + lx, 3),
                    "y": round(center[1] + ly, 3),
                    "z": round(center[2] + lz, 3),
                }
            )

    payload = {
        "source": "api",
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
        },
    }
    conn.close()
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
