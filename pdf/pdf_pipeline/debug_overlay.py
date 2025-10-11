from __future__ import annotations

from io import BytesIO
from typing import Dict, List, Optional

import pdfplumber
from PIL import Image, ImageDraw


def render_page_overlay(
    pdf_bytes: bytes,
    page_index: int,
    words: Optional[List[Dict[str, object]]] = None,
    column_bands: Optional[List[tuple]] = None,
    scale: float = 2.0,
) -> Image.Image:
    """
    Render a PNG overlay for a single PDF page, drawing word boxes and column bands.
    Coordinates from pdfplumber are used directly (origin top-left).
    """
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[page_index]
        page_image = page.to_image(resolution=int(72 * scale))
        img = page_image.image.convert("RGBA")
        draw = ImageDraw.Draw(img, "RGBA")

        # Column bands
        if column_bands:
            for (xmin, xmax) in column_bands:
                draw.rectangle([(xmin * scale, 0), (xmax * scale, img.height)], fill=(0, 128, 255, 40))

        # Word boxes
        if words:
            for w in words:
                x0 = float(w.get("x0", 0.0)) * scale
                y0 = float(w.get("top", 0.0)) * scale
                x1 = float(w.get("x1", 0.0)) * scale
                y1 = float(w.get("bottom", 0.0)) * scale
                draw.rectangle([(x0, y0), (x1, y1)], outline=(255, 0, 0, 180), width=1)
        return img


