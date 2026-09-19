"""Build the manual PDF from its reviewed Markdown source (requires reportlab)."""

import html
import re
import textwrap
from functools import partial
from pathlib import Path

import reportlab
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer

FONT_ROOT = Path(reportlab.__file__).parent / "fonts"
pdfmetrics.registerFont(TTFont("ManualSans", str(FONT_ROOT / "Vera.ttf")))
pdfmetrics.registerFont(TTFont("ManualBold", str(FONT_ROOT / "VeraBd.ttf")))
pdfmetrics.registerFont(TTFont("ManualMono", str(FONT_ROOT / "Vera.ttf")))

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/NLPIPE_User_Manual.pdf"
styles = getSampleStyleSheet()
styles.add(
    ParagraphStyle(
        name="ManualBody",
        fontName="ManualSans",
        fontSize=10.2,
        leading=15.2,
        textColor=colors.HexColor("#233247"),
        spaceAfter=9,
    )
)
styles.add(
    ParagraphStyle(
        name="ManualTitle",
        fontName="ManualBold",
        fontSize=31,
        leading=37,
        textColor=colors.HexColor("#102840"),
        spaceAfter=20,
    )
)
styles.add(
    ParagraphStyle(
        name="ManualHeading",
        fontName="ManualBold",
        fontSize=22,
        leading=27,
        textColor=colors.HexColor("#102840"),
        spaceAfter=17,
    )
)
styles.add(
    ParagraphStyle(
        name="ManualCode",
        fontName="ManualMono",
        fontSize=8.1,
        leading=11.5,
        backColor=colors.HexColor("#F0F4F7"),
        borderPadding=9,
        spaceBefore=5,
        spaceAfter=13,
    )
)
styles.add(
    ParagraphStyle(
        name="Eyebrow",
        fontName="ManualBold",
        fontSize=10,
        leading=15,
        textColor=colors.HexColor("#087F8C"),
        spaceAfter=17,
    )
)


def inline(text):
    escaped = html.escape(text)
    return re.sub(r"`([^`]+)`", r'<font name="ManualMono" size="9">\1</font>', escaped)


def footer(canvas, doc):
    canvas.saveState()
    width, height = doc.pagesize
    canvas.setStrokeColor(colors.HexColor("#DCE4EA"))
    canvas.line(54, 43, width - 54, 43)
    canvas.setFont("ManualSans", 8)
    canvas.setFillColor(colors.HexColor("#60758A"))
    canvas.drawString(54, 30, "NLPIPE / USER & OPERATOR MANUAL / 0.1.0")
    canvas.drawRightString(width - 54, 30, str(doc.page))
    canvas.restoreState()


story = [
    Spacer(1, 1.05 * inch),
    Paragraph("DATA ENGINEERING / OPEN SOURCE", styles["Eyebrow"]),
    Paragraph("Data Pipelines<br/>in Natural Language", styles["ManualTitle"]),
    Paragraph("User &amp; operator manual", styles["ManualHeading"]),
    Paragraph("Version 0.1.0 | September 2026", styles["ManualBody"]),
    Spacer(1, 0.35 * inch),
    Paragraph(
        "Describe the work. Review the specification.<br/>Validate the behavior. Deploy with evidence.",
        styles["ManualBody"],
    ),
    Spacer(1, 0.4 * inch),
    Paragraph(
        "A practical guide to installation, catalogs, model configuration, pipeline authoring, quality controls, approval, Airflow deployment, evaluation, and troubleshooting.",
        styles["ManualBody"],
    ),
    PageBreak(),
    Paragraph("Contents", styles["ManualHeading"]),
]
lines = (ROOT / "docs/USER_MANUAL.md").read_text().splitlines()
sections = [line[3:] for line in lines if re.match(r"^## \d+\.", line)]
for heading in sections:
    story.append(Paragraph(inline(heading), styles["ManualBody"]))
story.append(Spacer(1, 0.25 * inch))
story.append(
    Paragraph(
        "Read the release validation report alongside this manual. Offline corpus scores, mocked-provider tests, real-model checks, and Airflow execution establish different kinds of evidence.",
        styles["ManualBody"],
    )
)
started = False
paragraph = []
code = []
in_code = False


def flush():
    if paragraph:
        story.append(Paragraph(inline(" ".join(paragraph)), styles["ManualBody"]))
        paragraph.clear()


for line in lines:
    if re.match(r"^## \d+\.", line):
        flush()
        story.append(PageBreak())
        story.append(Paragraph(inline(line[3:]), styles["ManualHeading"]))
        started = True
    elif not started:
        continue
    elif line.startswith("```"):
        flush()
        if in_code:
            wrapped = []
            for source in code:
                wrapped.extend(
                    textwrap.wrap(
                        source,
                        width=88,
                        subsequent_indent="    ",
                        replace_whitespace=False,
                        drop_whitespace=False,
                    )
                    or [""]
                )
            story.append(Preformatted("\n".join(wrapped), styles["ManualCode"]))
            code = []
        in_code = not in_code
    elif in_code:
        code.append(line)
    elif not line.strip():
        flush()
    else:
        paragraph.append(line)
flush()
doc = SimpleDocTemplate(
    str(OUT),
    pagesize=(612, 792),
    leftMargin=54,
    rightMargin=54,
    topMargin=52,
    bottomMargin=58,
    title="Data Pipelines in Natural Language: User and Operator Manual",
    author="nlpipe contributors",
)
doc.build(story, onFirstPage=footer, onLaterPages=footer, canvasmaker=partial(Canvas, invariant=1))
print(OUT)
