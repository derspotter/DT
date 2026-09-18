#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OCR HTTP service using PaddleOCR with lazy load + hibernate endpoints.
- Accepts PDF or image uploads
- Loads OCR engine on-demand
- /load and /unload endpoints to move weights in/out of VRAM
- Optional model dir overrides for RAM-disk use
"""

import gc
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn

from paddleocr import PaddleOCR
import paddle
from PIL import Image, ImageOps
import pypdfium2 as pdfium

app = FastAPI(title="Rechtmaschine OCR Service (Hibernate)", version="1.1.0")


# --------- Configure OCR (lazy init) ----------

def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_path(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


USE_DOC_ORIENTATION = _env_flag("OCR_USE_DOC_ORIENTATION", True)
USE_DOC_UNWARPING = _env_flag("OCR_USE_UNWARPING", False)
USE_TEXTLINE_ORIENTATION = _env_flag("OCR_USE_TEXTLINE_ORIENTATION", True)
ENABLE_HPI = _env_flag("OCR_ENABLE_HPI", True)
OCR_LANG = os.getenv("OCR_LANG", "").strip()
HPI_BACKEND = os.getenv("OCR_HPI_BACKEND", "paddle").strip().lower()
RETURN_WORD_BOX = _env_flag("OCR_RETURN_WORD_BOX", False)
ENABLE_PDF_REPAIR = _env_flag("OCR_ENABLE_PDF_REPAIR", True)
PDF_REPAIR_TIMEOUT = int(os.getenv("OCR_PDF_REPAIR_TIMEOUT", "30"))
PDF_RENDER_DPI = int(os.getenv("OCR_PDF_RENDER_DPI", "200"))
ENABLE_DPI_FALLBACK = _env_flag("OCR_ENABLE_DPI_FALLBACK", True)
PDF_FALLBACK_DPI = int(os.getenv("OCR_PDF_FALLBACK_DPI", "150"))
PDF_FALLBACK_DPIS = os.getenv("OCR_PDF_FALLBACK_DPIS", "150,120,100")
PDF_MAX_RENDER_LONG_EDGE = int(os.getenv("OCR_PDF_MAX_RENDER_LONG_EDGE", "2600"))
PDF_MAX_RENDER_MEGAPIXELS = float(os.getenv("OCR_PDF_MAX_RENDER_MEGAPIXELS", "8.0"))
IMAGE_MAX_LONG_EDGE = int(os.getenv("OCR_IMAGE_MAX_LONG_EDGE", str(PDF_MAX_RENDER_LONG_EDGE)))
IMAGE_MAX_MEGAPIXELS = float(os.getenv("OCR_IMAGE_MAX_MEGAPIXELS", str(PDF_MAX_RENDER_MEGAPIXELS)))
ENABLE_VRAM_SAMPLING = _env_flag("OCR_ENABLE_VRAM_SAMPLING", True)
VRAM_SAMPLE_INTERVAL_SECONDS = float(os.getenv("OCR_VRAM_SAMPLE_INTERVAL_SECONDS", "0.25"))
REQUEST_ID_HEADER = "X-Request-ID"

MODEL_BASE_DIR = _env_path("OCR_MODEL_BASE_DIR")
DET_MODEL_DIR = _env_path("OCR_DET_MODEL_DIR")
REC_MODEL_DIR = _env_path("OCR_REC_MODEL_DIR")
CLS_MODEL_DIR = _env_path("OCR_CLS_MODEL_DIR")


def _resolve_model_dir(
    explicit: Optional[str], base_dir: Optional[str], subdir: str
) -> Optional[str]:
    if explicit:
        return explicit
    if not base_dir:
        return None
    candidate = os.path.join(base_dir, subdir)
    if os.path.isdir(candidate):
        return candidate
    print(f"[WARNING] OCR_MODEL_BASE_DIR set but missing '{subdir}' in {base_dir}")
    return None


DET_MODEL_DIR = _resolve_model_dir(DET_MODEL_DIR, MODEL_BASE_DIR, "det")
REC_MODEL_DIR = _resolve_model_dir(REC_MODEL_DIR, MODEL_BASE_DIR, "rec")
CLS_MODEL_DIR = _resolve_model_dir(CLS_MODEL_DIR, MODEL_BASE_DIR, "cls")

# 3.x-API: Modelle per Name wählen (z.B. PP-OCRv6_medium_det/_rec); die
# *_MODEL_DIR-Knöpfe oben stammen aus der 2.x-API und greifen dort nicht.
DET_MODEL_NAME = os.getenv("OCR_DET_MODEL_NAME", "").strip() or None
REC_MODEL_NAME = os.getenv("OCR_REC_MODEL_NAME", "").strip() or None

OCR_ENGINE: Optional[PaddleOCR] = None
OCR_ENGINE_LOCK = threading.Lock()
OCR_IN_FLIGHT = 0
OCR_IN_FLIGHT_LOCK = threading.Lock()
OCR_REQUEST_LOCK = threading.Lock()
OCR_LAST_LOAD_SECONDS: Optional[float] = None
OCR_LAST_UNLOAD_AT: Optional[float] = None

print("[INFO] OCR Engine configured (lazy load enabled)")
print(
    "[INFO] OCR config: "
    f"doc_orientation={USE_DOC_ORIENTATION}, "
    f"textline_orientation={USE_TEXTLINE_ORIENTATION}, "
    f"unwarping={USE_DOC_UNWARPING}, "
    f"hpi={ENABLE_HPI}, "
    f"hpi_backend={HPI_BACKEND or 'auto'}, "
    f"return_word_box={RETURN_WORD_BOX}, "
    f"pdf_render_dpi={PDF_RENDER_DPI}, "
    f"pdf_fallback_dpis={PDF_FALLBACK_DPIS if ENABLE_DPI_FALLBACK else 'disabled'}, "
    f"pdf_max_render_long_edge={PDF_MAX_RENDER_LONG_EDGE}, "
    f"pdf_max_render_megapixels={PDF_MAX_RENDER_MEGAPIXELS}, "
    f"image_max_long_edge={IMAGE_MAX_LONG_EDGE}, "
    f"image_max_megapixels={IMAGE_MAX_MEGAPIXELS}, "
    f"vram_sampling={ENABLE_VRAM_SAMPLING}"
)
if MODEL_BASE_DIR or DET_MODEL_DIR or REC_MODEL_DIR or CLS_MODEL_DIR:
    print(
        "[INFO] OCR model dirs: "
        f"base={MODEL_BASE_DIR or 'default'}, "
        f"det={DET_MODEL_DIR or 'default'}, "
        f"rec={REC_MODEL_DIR or 'default'}, "
        f"cls={CLS_MODEL_DIR or 'default'}"
    )
print(
    "[INFO] OCR models: "
    f"det={DET_MODEL_NAME or 'default'}, "
    f"rec={REC_MODEL_NAME or 'default'}, "
    f"lang={OCR_LANG or 'default'}"
)


def load_engine() -> PaddleOCR:
    global OCR_ENGINE, OCR_LAST_LOAD_SECONDS
    if OCR_ENGINE is not None:
        return OCR_ENGINE

    with OCR_ENGINE_LOCK:
        if OCR_ENGINE is not None:
            return OCR_ENGINE

        print("[INFO] Loading OCR Engine into VRAM...")
        start_time = time.time()

        model_kwargs: Dict[str, Any] = {}
        if DET_MODEL_DIR:
            model_kwargs["det_model_dir"] = DET_MODEL_DIR
        if REC_MODEL_DIR:
            model_kwargs["rec_model_dir"] = REC_MODEL_DIR
        if CLS_MODEL_DIR:
            model_kwargs["cls_model_dir"] = CLS_MODEL_DIR
        if ENABLE_HPI and HPI_BACKEND:
            model_kwargs["engine_config"] = {"backend": HPI_BACKEND}
        if OCR_LANG:
            model_kwargs["lang"] = OCR_LANG
        if DET_MODEL_NAME:
            model_kwargs["text_detection_model_name"] = DET_MODEL_NAME
        if REC_MODEL_NAME:
            model_kwargs["text_recognition_model_name"] = REC_MODEL_NAME

        OCR_ENGINE = PaddleOCR(
            use_doc_orientation_classify=USE_DOC_ORIENTATION,
            use_doc_unwarping=USE_DOC_UNWARPING,
            use_textline_orientation=USE_TEXTLINE_ORIENTATION,
            enable_hpi=ENABLE_HPI,
            return_word_box=RETURN_WORD_BOX,
            **model_kwargs,
        )

        OCR_LAST_LOAD_SECONDS = time.time() - start_time
        print(f"[INFO] OCR Engine initialized in {OCR_LAST_LOAD_SECONDS:.2f}s")

    return OCR_ENGINE


def _log(request_id: str, message: str) -> None:
    print(f"[request_id={request_id}] {message}")


def _is_oom_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in [
            "out of memory",
            "cannot allocate",
            "cuda error 2",
            "cuda out of memory",
            "resource exhausted",
        ]
    )


def _fallback_dpi_candidates() -> list[int]:
    if not ENABLE_DPI_FALLBACK:
        return [PDF_RENDER_DPI]

    candidates = [PDF_RENDER_DPI]
    raw_values = [str(PDF_FALLBACK_DPI), *PDF_FALLBACK_DPIS.split(",")]
    for raw_value in raw_values:
        raw_value = raw_value.strip()
        if not raw_value:
            continue
        try:
            dpi = int(raw_value)
        except ValueError:
            continue
        if dpi > 0 and dpi < PDF_RENDER_DPI and dpi not in candidates:
            candidates.append(dpi)
    return candidates


def _nvidia_memory_used_mb() -> Optional[int]:
    nvidia_smi = shutil.which("nvidia-smi")
    if not nvidia_smi:
        return None
    try:
        result = subprocess.run(
            [
                nvidia_smi,
                "--query-gpu=memory.used",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    values = []
    for line in result.stdout.splitlines():
        try:
            values.append(int(line.strip()))
        except ValueError:
            pass
    return max(values) if values else None


@dataclass
class VramSampler:
    request_id: str
    enabled: bool = ENABLE_VRAM_SAMPLING
    interval_seconds: float = VRAM_SAMPLE_INTERVAL_SECONDS
    baseline_mb: Optional[int] = None
    peak_mb: Optional[int] = None
    samples: int = 0
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.enabled:
            return
        self.baseline_mb = _nvidia_memory_used_mb()
        self.peak_mb = self.baseline_mb
        self.samples = 1 if self.baseline_mb is not None else 0
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        if not self.enabled:
            return {"enabled": False}
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=max(self.interval_seconds * 2, 1.0))
        final_mb = _nvidia_memory_used_mb()
        if final_mb is not None:
            self.samples += 1
            self.peak_mb = max(self.peak_mb or final_mb, final_mb)
        peak_delta_mb = None
        if self.baseline_mb is not None and self.peak_mb is not None:
            peak_delta_mb = max(0, self.peak_mb - self.baseline_mb)
        return {
            "enabled": True,
            "baseline_mb": self.baseline_mb,
            "peak_mb": self.peak_mb,
            "peak_delta_mb": peak_delta_mb,
            "final_mb": final_mb,
            "samples": self.samples,
        }

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            used_mb = _nvidia_memory_used_mb()
            if used_mb is None:
                continue
            self.samples += 1
            self.peak_mb = max(self.peak_mb or used_mb, used_mb)


def _clear_cuda_cache() -> None:
    gc.collect()
    try:
        paddle.device.cuda.empty_cache()
    except Exception as exc:
        print(f"[WARNING] Failed to clear CUDA cache: {exc}")


def _reset_engine_after_oom(request_id: str) -> PaddleOCR:
    global OCR_ENGINE
    _log(request_id, "[WARNING] Resetting OCR engine after GPU OOM")
    with OCR_ENGINE_LOCK:
        OCR_ENGINE = None
    _clear_cuda_cache()
    return load_engine()


def _map_point_from_rotated(x: float, y: float, angle: int, width: float, height: float):
    """Map a point from the doc-preprocessor-rotated frame back to the input frame.

    `angle` is the counterclockwise rotation the preprocessor applied to the
    input image (width x height) before detection.
    """
    if angle == 90:
        return (width - 1 - y, x)
    if angle == 180:
        return (width - 1 - x, height - 1 - y)
    if angle == 270:
        return (y, height - 1 - x)
    return (x, y)


def _rotate_box_tree(value: Any, angle: int, width: float, height: float) -> Any:
    """Apply _map_point_from_rotated across nested box structures."""
    if not isinstance(value, (list, tuple)):
        return value
    items = list(value)
    if items and all(isinstance(v, (int, float)) for v in items) and len(items) % 2 == 0:
        out: list[float] = []
        for i in range(0, len(items), 2):
            x, y = _map_point_from_rotated(items[i], items[i + 1], angle, width, height)
            out.extend([x, y])
        return out
    return [_rotate_box_tree(item, angle, width, height) for item in items]


def _scale_box_tree(value: Any, fx: float, fy: float) -> Any:
    """Rescale nested box structures (point pairs or flat x0,y0,x1,y1 quads)."""
    if not isinstance(value, (list, tuple)):
        return value
    items = list(value)
    if items and all(isinstance(v, (int, float)) for v in items):
        if len(items) % 2 == 0:
            return [
                v * (fx if i % 2 == 0 else fy)
                for i, v in enumerate(items)
            ]
        return items
    return [_scale_box_tree(item, fx, fy) for item in items]


def _prediction_result_to_page(
    page_result: Any,
    page_index: int,
    request_id: str,
    metadata: Optional[dict[str, Any]] = None,
) -> Dict[str, Any]:
    page_data = page_result.json["res"]
    lines = page_data.get("rec_texts", [])
    scores = page_data.get("rec_scores", [])
    boxes = page_data.get("rec_polys", [])
    text_word = page_data.get("text_word")
    word_boxes = page_data.get("text_word_boxes")

    meta = metadata or {}

    # If the doc-orientation preprocessor rotated the image, PaddleOCR returns
    # boxes in the rotated frame. Map them back to the input frame first.
    try:
        doc_angle = int(((page_data.get("doc_preprocessor_res") or {}).get("angle")) or 0) % 360
    except (TypeError, ValueError):
        doc_angle = 0
    if doc_angle in (90, 180, 270):
        in_w = meta.get("image_width") or (meta.get("render_size") or {}).get("width")
        in_h = meta.get("image_height") or (meta.get("render_size") or {}).get("height")
        if in_w and in_h:
            boxes = _rotate_box_tree(boxes, doc_angle, float(in_w), float(in_h))
            if word_boxes is not None:
                word_boxes = _rotate_box_tree(word_boxes, doc_angle, float(in_w), float(in_h))
            meta = dict(meta)
            meta["doc_rotation_angle"] = doc_angle
            _log(
                request_id,
                f"[INFO] Page {page_index}: mapped boxes back from doc rotation angle={doc_angle}",
            )

    # If the image was downscaled before OCR, map boxes back to the original
    # image coordinate space so consumers (e.g. hOCR generation) can rely on
    # box coordinates matching the input image dimensions.
    if meta.get("image_bounded"):
        try:
            fx = float(meta["image_original_width"]) / float(meta["image_width"])
            fy = float(meta["image_original_height"]) / float(meta["image_height"])
        except (KeyError, TypeError, ZeroDivisionError):
            fx = fy = 1.0
        if abs(fx - 1.0) > 1e-6 or abs(fy - 1.0) > 1e-6:
            boxes = _scale_box_tree(boxes, fx, fy)
            if word_boxes is not None:
                word_boxes = _scale_box_tree(word_boxes, fx, fy)

    _log(request_id, f"[INFO] Page {page_index}: extracted {len(lines)} lines")
    return {
        "page_index": page_index,
        "lines": lines,
        "confidences": scores,
        "boxes": boxes,
        "text_word": text_word,
        "word_boxes": word_boxes,
        "line_count": len(lines),
        "metadata": meta,
    }


def _predict_image_path(engine: PaddleOCR, image_path: str) -> list[Any]:
    result = engine.predict(input=image_path)
    if result is None:
        return []
    return list(result)


def _is_image_file(file_path: str) -> bool:
    suffix = Path(file_path).suffix.lower()
    return suffix in {".bmp", ".gif", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}


@contextmanager
def _bounded_image_for_ocr(file_path: str, request_id: str):
    if not _is_image_file(file_path):
        yield file_path, {
            "image_bounded": False,
            "image_original_width": None,
            "image_original_height": None,
            "image_width": None,
            "image_height": None,
        }
        return

    tmp_path: Optional[str] = None
    try:
        with Image.open(file_path) as image:
            image = ImageOps.exif_transpose(image)
            if image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")

            original_width, original_height = image.size
            scale = 1.0
            if IMAGE_MAX_LONG_EDGE > 0:
                long_edge = max(original_width, original_height)
                if long_edge > IMAGE_MAX_LONG_EDGE:
                    scale = min(scale, IMAGE_MAX_LONG_EDGE / long_edge)

            if IMAGE_MAX_MEGAPIXELS > 0:
                megapixels = (original_width * original_height) / 1_000_000
                if megapixels > IMAGE_MAX_MEGAPIXELS:
                    scale = min(scale, (IMAGE_MAX_MEGAPIXELS / megapixels) ** 0.5)

            if scale >= 0.999:
                yield file_path, {
                    "image_bounded": False,
                    "image_original_width": original_width,
                    "image_original_height": original_height,
                    "image_width": original_width,
                    "image_height": original_height,
                }
                return

            width = max(1, int(original_width * scale))
            height = max(1, int(original_height * scale))
            image = image.resize((width, height), Image.Resampling.LANCZOS)
            handle, tmp_path = tempfile.mkstemp(suffix=".png")
            os.close(handle)
            image.save(tmp_path)
            _log(
                request_id,
                "[INFO] Resized image for OCR "
                f"{original_width}x{original_height} -> {width}x{height}",
            )
            yield tmp_path, {
                "image_bounded": True,
                "image_original_width": original_width,
                "image_original_height": original_height,
                "image_width": width,
                "image_height": height,
                "image_scale": round(scale, 4),
            }
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass


def _render_pdf_page_to_png(
    page: Any,
    page_index: int,
    tmp_dir: str,
    dpi: int,
    request_id: str,
) -> tuple[str, dict[str, Any]]:
    start_time = time.perf_counter()
    page_width_pt, page_height_pt = page.get_size()
    requested_scale = dpi / 72.0
    requested_width = int(page_width_pt * requested_scale)
    requested_height = int(page_height_pt * requested_scale)
    scale = requested_scale
    render_limited = False

    if PDF_MAX_RENDER_LONG_EDGE > 0:
        requested_long_edge = max(requested_width, requested_height)
        if requested_long_edge > PDF_MAX_RENDER_LONG_EDGE:
            scale *= PDF_MAX_RENDER_LONG_EDGE / requested_long_edge
            render_limited = True

    if PDF_MAX_RENDER_MEGAPIXELS > 0:
        capped_width = page_width_pt * scale
        capped_height = page_height_pt * scale
        capped_megapixels = (capped_width * capped_height) / 1_000_000
        if capped_megapixels > PDF_MAX_RENDER_MEGAPIXELS:
            scale *= (PDF_MAX_RENDER_MEGAPIXELS / capped_megapixels) ** 0.5
            render_limited = True

    effective_dpi = scale * 72.0
    output_path = os.path.join(tmp_dir, f"page_{page_index:04d}_{dpi}dpi.png")
    bitmap = page.render(scale=scale)
    image = bitmap.to_pil()
    width, height = image.size
    image.save(output_path)
    image.close()
    bitmap.close()
    elapsed_seconds = time.perf_counter() - start_time
    _log(
        request_id,
        "[TIMING] OCR page render "
        f"page={page_index} requested_dpi={dpi} "
        f"effective_dpi={effective_dpi:.1f} "
        f"size={width}x{height} limited={render_limited} "
        f"seconds={elapsed_seconds:.3f}"
    )
    return output_path, {
        "render_seconds": round(elapsed_seconds, 3),
        "render_dpi": dpi,
        "effective_dpi": round(effective_dpi, 1),
        "render_size": {"width": width, "height": height},
        "requested_render_size": {
            "width": requested_width,
            "height": requested_height,
        },
        "render_limited": render_limited,
    }


def _empty_page_result(
    page_index: int,
    request_id: str,
    metadata: Optional[dict[str, Any]] = None,
) -> Dict[str, Any]:
    _log(request_id, f"[INFO] Page {page_index}: extracted 0 lines")
    return {
        "page_index": page_index,
        "lines": [],
        "confidences": [],
        "boxes": [],
        "text_word": None,
        "word_boxes": None,
        "line_count": 0,
        "metadata": metadata or {},
    }


def _ocr_rendered_page(
    engine: PaddleOCR,
    page: Any,
    page_index: int,
    page_count: int,
    tmp_dir: str,
    request_id: str,
) -> Dict[str, Any]:
    dpi_candidates = _fallback_dpi_candidates()

    last_exception: Optional[Exception] = None
    for attempt_index, dpi in enumerate(dpi_candidates, start=1):
        rendered_path = None
        page_start_time = time.perf_counter()
        try:
            rendered_path, page_metadata = _render_pdf_page_to_png(
                page, page_index, tmp_dir, dpi, request_id
            )
            predict_start_time = time.perf_counter()
            predictions = _predict_image_path(engine, rendered_path)
            predict_seconds = time.perf_counter() - predict_start_time
            total_page_seconds = time.perf_counter() - page_start_time

            page_metadata.update(
                {
                    "predict_seconds": round(predict_seconds, 3),
                    "total_seconds": round(total_page_seconds, 3),
                    "attempt": attempt_index,
                    "fallback_used": dpi != PDF_RENDER_DPI,
                }
            )

            if predictions:
                page_result = _prediction_result_to_page(
                    predictions[0], page_index, request_id, page_metadata
                )
            else:
                page_result = _empty_page_result(page_index, request_id, page_metadata)

            _log(
                request_id,
                "[TIMING] OCR page total "
                f"page={page_index}/{page_count} "
                f"dpi={dpi} predict_seconds={predict_seconds:.3f} "
                f"total_seconds={total_page_seconds:.3f}"
            )
            return page_result
        except Exception as exc:
            last_exception = exc
            if not _is_oom_error(exc) or dpi == dpi_candidates[-1]:
                raise
            next_dpi = dpi_candidates[attempt_index]
            _log(
                request_id,
                "[WARNING] OCR page failed with probable GPU OOM; "
                f"retrying page={page_index} at dpi={next_dpi}"
            )
            engine = _reset_engine_after_oom(request_id)
        finally:
            if rendered_path:
                try:
                    os.unlink(rendered_path)
                except FileNotFoundError:
                    pass

    if last_exception:
        raise last_exception
    raise RuntimeError(f"OCR page {page_index} failed without result")


def _ocr_pdf_page_by_page(file_path: str, request_id: str) -> list[Dict[str, Any]]:
    total_start_time = time.perf_counter()
    file_size_bytes = os.path.getsize(file_path)
    _log(
        request_id,
        "[INFO] Running page-by-page PDF OCR on: "
        f"{file_path} ({file_size_bytes} bytes)"
    )
    engine = load_engine()
    page_results: list[Dict[str, Any]] = []

    document = None
    with tempfile.TemporaryDirectory(prefix="ocr_pdf_pages_") as tmp_dir:
        document = pdfium.PdfDocument(file_path)
        page_count = len(document)
        _log(
            request_id,
            f"[INFO] PDF has {page_count} page(s); processing one page at a time",
        )

        try:
            for zero_based_index in range(page_count):
                page_index = zero_based_index + 1
                page = None
                try:
                    page = document[zero_based_index]
                    page_results.append(
                        _ocr_rendered_page(
                            engine, page, page_index, page_count, tmp_dir, request_id
                        )
                    )
                finally:
                    if page is not None:
                        page.close()
                    _clear_cuda_cache()
        finally:
            if document is not None:
                document.close()

    total_seconds = time.perf_counter() - total_start_time
    seconds_per_page = total_seconds / page_count if page_count else 0.0
    total_lines = sum(len(page.get("lines", [])) for page in page_results)
    _log(
        request_id,
        "[TIMING] OCR document total "
        f"pages={page_count} lines={total_lines} "
        f"bytes={file_size_bytes} total_seconds={total_seconds:.3f} "
        f"seconds_per_page={seconds_per_page:.3f}"
    )
    return page_results


def _ocr_file_path(file_path: str, request_id: str):
    """
    Run OCR on a file (image or PDF).
    Uses page-by-page rendering for PDFs to keep GPU peak memory bounded.
    """
    _log(request_id, f"[INFO] Running OCR on: {file_path}")
    is_pdf = file_path.lower().endswith(".pdf")
    if is_pdf:
        try:
            return _ocr_pdf_page_by_page(file_path, request_id)
        except Exception as exc:
            _log(request_id, f"[WARNING] Page-by-page PDF OCR failed for {file_path}: {exc}")
            repaired_path = _repair_pdf(file_path)
            if not repaired_path:
                raise
            try:
                _log(request_id, "[INFO] Retrying page-by-page OCR with repaired PDF...")
                return _ocr_pdf_page_by_page(repaired_path, request_id)
            finally:
                try:
                    os.unlink(repaired_path)
                except FileNotFoundError:
                    pass

    engine = load_engine()
    with _bounded_image_for_ocr(file_path, request_id) as (ocr_image_path, image_metadata):
        image_start_time = time.perf_counter()
        predictions = _predict_image_path(engine, ocr_image_path)
        image_total_seconds = time.perf_counter() - image_start_time
        if not predictions:
            return []
        _log(
            request_id,
            "[TIMING] OCR image total "
            f"pages={len(predictions)} total_seconds={image_total_seconds:.3f}",
        )
        return [
            _prediction_result_to_page(
                page_result,
                page_idx,
                request_id,
                {
                    **image_metadata,
                    "predict_seconds": round(image_total_seconds, 3),
                    "total_seconds": round(image_total_seconds, 3),
                    "attempt": 1,
                    "fallback_used": False,
                },
            )
            for page_idx, page_result in enumerate(predictions, 1)
        ]


def _repair_pdf(file_path: str) -> Optional[str]:
    if not ENABLE_PDF_REPAIR:
        return None

    qpdf_path = shutil.which("qpdf")
    if not qpdf_path:
        print("[WARNING] qpdf not found; skipping PDF repair pass.")
        return None

    repaired_handle, repaired_path = tempfile.mkstemp(suffix=".pdf")
    os.close(repaired_handle)

    try:
        result = subprocess.run(
            [qpdf_path, file_path, repaired_path],
            capture_output=True,
            text=True,
            timeout=PDF_REPAIR_TIMEOUT,
        )
    except Exception as exc:
        print(f"[WARNING] PDF repair failed: {exc}")
        try:
            os.unlink(repaired_path)
        except FileNotFoundError:
            pass
        return None

    if result.returncode != 0:
        stderr = result.stderr.strip()
        print(f"[WARNING] PDF repair failed (exit {result.returncode}): {stderr}")
        try:
            os.unlink(repaired_path)
        except FileNotFoundError:
            pass
        return None

    print("[INFO] PDF repair completed successfully.")
    return repaired_path


@app.get("/health")
def health():
    return {
        "service": app.title,
        "version": app.version,
        "paddle_version": paddle.__version__,
        "gpu_available": paddle.is_compiled_with_cuda(),
        "engine_loaded": OCR_ENGINE is not None,
        "last_load_seconds": OCR_LAST_LOAD_SECONDS,
        "model_dirs": {
            "base": MODEL_BASE_DIR,
            "det": DET_MODEL_DIR,
            "rec": REC_MODEL_DIR,
            "cls": CLS_MODEL_DIR,
        },
    }


@app.post("/load")
def load():
    engine = load_engine()
    return {
        "status": "loaded",
        "engine_loaded": engine is not None,
        "last_load_seconds": OCR_LAST_LOAD_SECONDS,
    }


@app.post("/unload")
def unload():
    global OCR_ENGINE, OCR_LAST_UNLOAD_AT

    with OCR_IN_FLIGHT_LOCK:
        if OCR_IN_FLIGHT > 0:
            raise HTTPException(
                status_code=409,
                detail=f"OCR busy ({OCR_IN_FLIGHT} in-flight request(s))",
            )

    with OCR_ENGINE_LOCK:
        if OCR_ENGINE is None:
            return {"status": "already_unloaded", "engine_loaded": False}
        OCR_ENGINE = None

    gc.collect()
    if paddle.is_compiled_with_cuda():
        try:
            paddle.device.cuda.empty_cache()
        except Exception as exc:
            print(f"[WARNING] Failed to clear CUDA cache: {exc}")

    OCR_LAST_UNLOAD_AT = time.time()
    print("[INFO] OCR Engine unloaded from VRAM")
    return {"status": "unloaded", "engine_loaded": False}


@app.post("/ocr")
async def ocr_endpoint(request: Request, file: UploadFile = File(...)):
    request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex
    # Basic validation by extension; robust code should also sniff MIME.
    filename = (file.filename or "").lower()
    if not filename:
        raise HTTPException(400, "No filename provided.")
    _, ext = os.path.splitext(filename)
    if ext not in [".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".pdf"]:
        raise HTTPException(400, f"Unsupported extension: {ext}")

    # Stream upload to a temp file to avoid big RAM spikes.
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmpf:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            tmpf.write(chunk)
        tmp_path = tmpf.name

    global OCR_IN_FLIGHT
    request_start_time = time.perf_counter()
    vram_sampler = VramSampler(request_id=request_id)
    vram_summary: dict[str, Any] = {"enabled": ENABLE_VRAM_SAMPLING}
    try:
        _log(request_id, f"[INFO] OCR request received filename={filename}")
        with OCR_REQUEST_LOCK:
            vram_sampler.start()
            with OCR_IN_FLIGHT_LOCK:
                OCR_IN_FLIGHT += 1

            try:
                # PaddleOCR handles both images and PDFs directly
                page_results = _ocr_file_path(tmp_path, request_id)
            finally:
                with OCR_IN_FLIGHT_LOCK:
                    OCR_IN_FLIGHT -= 1
                vram_summary = vram_sampler.stop()

        # Aggregate
        all_lines: List[str] = []
        all_scores: List[float] = []
        for p in page_results:
            all_lines.extend(p["lines"])
            all_scores.extend(p["confidences"])

        full_text = "\n".join(all_lines)
        avg_conf = (sum(all_scores) / len(all_scores)) if all_scores else 0.0
        total_seconds = time.perf_counter() - request_start_time
        _log(
            request_id,
            "[TIMING] OCR request total "
            f"filename={filename} pages={len(page_results)} "
            f"total_seconds={total_seconds:.3f} "
            f"peak_vram_mb={vram_summary.get('peak_mb')} "
            f"peak_vram_delta_mb={vram_summary.get('peak_delta_mb')}",
        )

        return JSONResponse(
            {
                "request_id": request_id,
                "filename": filename,
                "page_count": len(page_results),
                "avg_confidence": round(avg_conf, 4),
                "full_text": full_text,
                "pages": page_results,
                "metadata": {
                    "total_seconds": round(total_seconds, 3),
                    "vram": vram_summary,
                    "serialized": True,
                    "hpi_enabled": ENABLE_HPI,
                    "hpi_backend": HPI_BACKEND,
                    "pdf_render_dpi": PDF_RENDER_DPI,
                    "pdf_fallback_dpis": _fallback_dpi_candidates()[1:],
                    "pdf_max_render_long_edge": PDF_MAX_RENDER_LONG_EDGE,
                    "pdf_max_render_megapixels": PDF_MAX_RENDER_MEGAPIXELS,
                },
            }
        )

    finally:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    # Bind to all interfaces; change as you wish.
    uvicorn.run("ocr_service_hibernate:app", host="0.0.0.0", port=9003, reload=False)
