import csv
import json
import ntpath
import os

import xlrd
from cubes import Workspace
from helper import create_table_from_csv
from sqlalchemy import create_engine


def build(db):
    datadir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    files = [os.path.join("data", f) for f in os.listdir(datadir)]
    build_models(files)
    for f in files:
        build_table(f, db)


def build_table(f, db):
    sh = get_sheet(f)
    csv_name = "_temp_file.csv"
    your_csv_file = open(csv_name, 'w')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)
    for rownum in range(sh.nrows):
        wr.writerow(sh.row_values(rownum))
    your_csv_file.close()

    engine = create_engine(db)

    cols, types = get_columns(f)
    cubes = [cube_name(col) for col in cols]

    mapper = {str: "string", float: "float", bool: "boolean"}
    fields = [(col, mapper[type_]) for col, type_ in zip(cols, types)]

    for cube in cubes:
        print(cube)
        create_table_from_csv(
            engine, csv_name, table_name=cube, fields=fields, create_id=True)

    os.remove(csv_name)


def build_models(files):
    model = {}
    for f in files:
        model.update(build_model(f))
    with open("model.json", "w") as f:
        f.write(json.dumps(model, indent=2, sort_keys=True))


def build_model(f):
    cols, types = get_columns(f)
    return {
        "cubes": [build_cube(f, type_, col, cols) for col, type_ in zip(cols, types)],
        "dimensions": build_dimensions(f, cols),
    }


def get_columns(f):
    worksheet = get_sheet(f)
    cols = [worksheet.cell_value(0, col_num) for col_num in range(0, worksheet.ncols)]
    types = [get_type(worksheet, col_num) for col_num in range(0, worksheet.ncols)]
    return cols, types


def get_type(worksheet, column):
    types = {1: str, 2: float, 4: bool}
    for row in range(1, 100):
        type_ = types.get(worksheet.cell_type(row, column))
        if type_:
            return type_


def build_cube(f, col_type, col_name, all_cols):
    type_args = {
        bool: ["count"],
        float: ["count", "avg", "max", "min"],
        str: ["count"],
    }
    return {
        "name": cube_name(col_name),
        "label": col_name,
        "dimensions": [dimension_name(f, col) for col in all_cols],
        "measures": [{"name": col_name, "label": col_name}],
        "aggregates": build_aggregates(col_name, type_args[col_type]),
    }


def build_aggregates(column, args):
    return [{
        "name": "{}_{}".format(column, arg),
        "function": arg,
        "measure": column,
        "label": arg,
    } for arg in args]


def build_dimensions(f, all_cols):
    return [{
        "name": dimension_name(f, col),
        "levels": [build_level(col)],
        "label": col,
    } for col in all_cols]


def build_level(col):
    return {
        "name": col,
        "label": col,
        "attributes": [col]
    }


def cube_name(col):
    return "{}_cube".format(col.replace(" ", "_"))


def dimension_name(f, col):
    fbase = ntpath.basename(f).split(".")[0]
    return "{}_{}_dimension".format(fbase, col).replace(" ", "_")


def get_sheet(f):
    wb = xlrd.open_workbook(f)
    return wb.sheet_by_name(wb.sheet_names()[0])


if __name__ == "__main__":
    build("sqlite:///data.sqlite")
