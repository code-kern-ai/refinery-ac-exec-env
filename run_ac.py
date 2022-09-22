import json
import requests
import spacy
import sys
from spacy.tokens import DocBin


def load_data_dict(record):
    if record["bytes"][:2] == "\\x":
        record["bytes"] = record["bytes"][2:]
    else:
        raise ValueError("Unknown byte format in DocBin. Please contact the support.")

    byte = bytes.fromhex(record["bytes"])
    doc_bin_loaded = DocBin().from_bytes(byte)
    docs = list(doc_bin_loaded.get_docs(vocab))
    data_dict = {}
    for (col, doc) in zip(record["columns"], docs):
        data_dict[col] = doc

    for key in record:
        if key in ["record_id", "bytes", "columns"]:
            continue
        data_dict[key] = record[key]
    return data_dict


def parse_data_to_record_dict(record_chunk):
    result = []
    for r in record_chunk:
        result.append({"id": r["record_id"], "data": load_data_dict(r)})
    return result


if __name__ == "__main__":
    _, iso2_code, payload_url = sys.argv

    print("Preparing data for attribute calculation.")

    # This import statement will always be highlighted as a potential error, as during devtime,
    # the script `labeling_functions` does not exist. It will be inserted at runtime
    from attribute_calculators import ac

    vocab = spacy.blank(iso2_code).vocab

    with open("docbin_full.json", "r") as infile:
        docbin_data = json.load(infile)

    record_dict_list = parse_data_to_record_dict(docbin_data)

    print("Running attribute calculation.")
    calculated_attribute_by_record_id = {}
    for record_dict in record_dict_list:
        calculated_attribute_by_record_id[record_dict["id"]] = ac(record_dict["data"])

    print("Finished execution.")
    requests.put(payload_url, json=calculated_attribute_by_record_id)
