# FILE: engine/work/run_denormalize_branches_prompt.py  (обновлено — 2026-01-08)
# PURPOSE: Прогоняет заданный немецкий текст через denormalize_branches_prompt() и печатает результат в stdout.

from engine.common.prompts.process import denormalize_branches_prompt


TEXT = """Neubau: Innenbau- und Ausbauarbeiten an Wohn-/Nichtwohngebäuden; schlüsselfertiger oder teilweiser Innenausbau; Maler- und Lackierarbeiten; Verlegung und Aufarbeitung von Parkett-, Dielen-, Vinyl- und Laminatböden; Badsanierung inkl. Sanitär-, Wasser-/Abwasserinstallation und Fliesenarbeiten; elektrotechnische Arbeiten; Kellerabdichtung; Positionierung im günstigen Segment mit gutem Preis-Leistungs-Verhältnis. Hände und Werke, Handwerksunternehmen für Haus-, Wohnungs- und Badsanierung mit allen Gewerken aus einer Hand; Adresse: Steinbeker Markt 6, 22117 Hamburg; tätig seit 1999 (über 25 Jahre Erfahrung); Website: https://www.haendeundwerke.de/. Bauarbeiten werden / Dienstleistungen werden in Hamburg und im Umkreis von bis zu 30 km erbracht. In anderen Regionen Deutschlands führt das Unternehmen keine Bauarbeiten aus und bietet dort keine Dienstleistungen an.
1 – Betriebe mit direktem Bedarf an Innenausbau-, Sanierungs- oder Ausbauleistungen an Gebäuden
2 – Branchen mit regelmäßigem oder wiederkehrendem Sanierungs- und Modernisierungsbedarf
3 – Unternehmen, die Endkundenprojekte in Wohn- oder Nichtwohngebäuden planen oder koordinieren
4 – Branchen mit hoher Dichte an Bestandsimmobilien im niedrigen bis mittleren Preissegment
5 – Kategorien, in denen Auftraggeber häufig Bau- oder Ausbauleistungen an externe Handwerker vergeben.
"""


def main() -> None:
    out = denormalize_branches_prompt(TEXT)
    print(out)


if __name__ == "__main__":
    main()
