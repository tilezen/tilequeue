from ModestMaps.Core import Coordinate
import csv


def parse_top_tiles(fp, zoom_start, zoom_until):
    coords = []
    reader = csv.reader(fp)
    for row in reader:
        try:
            zoom = int(row[0])
            column = int(row[1])
            row = int(row[2])
        except (ValueError, IndexError):
            continue
        else:
            if zoom_start <= zoom <= zoom_until:
                coord = Coordinate(
                    zoom=zoom,
                    column=column,
                    row=row,
                )
                coords.append(coord)

    return coords
