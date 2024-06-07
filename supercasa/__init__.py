"""
LOOP districts
    data = get(https://supercasa.pt/arrendar-casas/porto-distrito/zonas) (district)
    municipios = list(xpath id(AllTownsView))
    get href for each municipio
    filter links by regex href= /arrendar-casas/$district/(...)
    parse
    <a data-id="889827" data-layer="190" href="/arrendar-casas/porto/cedofeita-santo-ildefonso-se-miragaia-sao-nicolau-e-vitoria">Cedofeita, Santo Ildefonso, Sé, Miragaia, São Nicolau e Vitória <span>(114)</span></a>
    data-id, data-layer, value and href
    LOOP municipios
        parish = get(https://supercasa.pt/arrendar-casas/vila-nova-de-gaia/zonas)

return a dataframe of all data-id, data-layer, href and value
"""