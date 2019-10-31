import os, pandas
import math
from biothings.utils.dataload import dict_sweep


def load_data_products(data_folder):
    products_file = os.path.join(data_folder,"Products.csv")
    assert os.path.exists(products_file)
    dat = pandas.read_csv(products_file,squeeze=True).to_dict(orient='records')
    for rec in dat:
        rec["_id"] = str(rec["NDB_Number"])
        rec["manufacturer"] = str(rec["manufacturer"])
        rec["ingredients_english"] = str(rec["ingredients_english"])
        rec.pop("gtin_upc",None)
        yield rec


def load_data_nutrients(data_folder):
    products_file = os.path.join(data_folder,"Nutrients.csv")
    assert os.path.exists(products_file)
    dat = pandas.read_csv(products_file,squeeze=True).to_dict(orient='records')
    combined = {"nutrients" : [], "_id" : str(dat[0]["NDB_No"])}
    print(combined)
    for rec in dat:
        recid = str(rec["NDB_No"])

        if recid != combined.get("_id"):
            yield combined
            combined = {"_id" : recid, "nutrients" : []}

        combined["nutrients"].append(
                {
                    "code" : rec["Nutrient_Code"],
                    "name" : rec["Nutrient_name"],
                    "value" : rec["Output_value"],
                    "uom" : rec["Output_uom"]
                    })
    if combined:
        yield combined


def load_data_sizes(data_folder):
    products_file = os.path.join(data_folder,"Serving_size.csv")
    assert os.path.exists(products_file)
    dat = pandas.read_csv(products_file,squeeze=True).to_dict(orient='records')
    for rec in dat:
        rec["_id"] = str(rec.pop("NDB_No"))
        rec["Household_Serving_Size_UOM"] = str(rec["Household_Serving_Size_UOM"])
        rec["Household_Serving_Size"] = str(rec["Household_Serving_Size"])
        rec = dict_sweep(rec,vals=["none","nan"])
        rec.pop("Preparation_State")
        yield rec


if __name__ == "__main__":
    load_data("/data/biothings_studio/datasources/bfpdb/2018-08-14")
