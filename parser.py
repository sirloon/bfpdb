import os, pandas

def load_data(data_folder):
    products_file = os.path.join(data_folder,"Products.csv")
    assert os.path.exists(products_file)
    dat = pandas.read_csv(products_file,squeeze=True).to_dict(orient='records')
    for rec in dat:
        rec["_id"] = str(rec["NDB_Number"])
        rec["manufacturer"] = str(rec["manufacturer"])
        rec["ingredients_english"] = str(rec["ingredients_english"])
        rec.pop("gtin_upc",None)
        yield rec


if __name__ == "__main__":
    load_data("/data/biothings_studio/datasources/bfpdb/2018-08-14")
