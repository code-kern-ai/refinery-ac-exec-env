from typing import Any, Dict, List, Generator, Callable, Tuple, Type
import asyncio
import json
import requests
import spacy
import sys
from mustache import prepare_and_render_mustache
from spacy.tokens import DocBin


def get_check_data_type_function(data_type: str) -> Tuple[List[Type], Callable]:
    if data_type == "INTEGER":
        return [int], __check_data_type_integer
    elif data_type == "FLOAT":
        return [int, float], __check_data_type_float
    elif data_type == "BOOLEAN":
        return [bool], __check_data_type_boolean
    elif data_type == "CATEGORY":
        return [str], __check_data_type_category
    elif data_type == "TEXT":
        return [str], __check_data_type_text
    elif data_type == "EMBEDDING_LIST":
        return [list], __check_data_type_embedding_list
    elif data_type == "LLM_RESPONSE":
        return [str], __check_data_type_text
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def __check_data_type_integer(attr_value: Any) -> bool:
    if attr_value is not None and not isinstance(attr_value, int):
        return False
    return True


def __check_data_type_float(attr_value: Any) -> bool:
    if (
        attr_value is not None
        and not isinstance(attr_value, float)
        and not isinstance(attr_value, int)
    ):
        return False
    return True


def __check_data_type_boolean(attr_value: Any) -> bool:
    if not isinstance(attr_value, bool):
        return False
    return True


def __check_data_type_category(attr_value: Any) -> bool:
    if not isinstance(attr_value, str):
        return False
    if attr_value == "":
        raise ValueError("Category cannot be empty string")
    return True


def __check_data_type_text(attr_value: Any) -> bool:
    if not isinstance(attr_value, str):
        return False
    return True


def __check_data_type_embedding_list(attr_value: Any) -> bool:
    if not isinstance(attr_value, list):
        return False
    for e in attr_value:
        if not isinstance(e, str) or len(e) == 0:
            raise ValueError("List entries need to be strings with a length > 0.")
    return True


def __print_progress(progress: float) -> None:
    print(f"progress: {progress}", flush=True)


def load_data_dict(record: Dict[str, Any]) -> Dict[str, Any]:
    global vocab

    if record["bytes"][:2] == "\\x":
        record["bytes"] = record["bytes"][2:]
    else:
        raise ValueError("Unknown byte format in DocBin. Please contact the support.")

    byte = bytes.fromhex(record["bytes"])
    doc_bin_loaded = DocBin().from_bytes(byte)
    docs = list(doc_bin_loaded.get_docs(vocab))
    data_dict = {}
    for col, doc in zip(record["columns"], docs):
        data_dict[col] = doc

    for key in record:
        if key in ["record_id", "bytes", "columns"]:
            continue
        data_dict[key] = record[key]
    return data_dict


def parse_data_to_record_dict(
    record_chunk: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    result = []
    for r in record_chunk:
        result.append({"id": r["record_id"], "data": load_data_dict(r)})
    return result


def save_ac_value(record_id: str, attr_value: Any) -> None:
    global calculated_attribute_by_record_id, processed_records, progress_size, amount, check_data_type, py_data_types

    if not check_data_type(attr_value):
        raise ValueError(
            f"Attribute value `{attr_value}` is of type {type(attr_value)}, "
            f"but data_type {data_type} requires "
            f"{str(py_data_types) if len(py_data_types) > 1 else str(py_data_types[0])}."
        )

    calculated_attribute_by_record_id[record_id] = attr_value

    processed_records = processed_records + 1
    if processed_records % progress_size == 0:
        __print_progress(round(processed_records / amount, 2))


def process_attribute_calculation(record_dict_list: List[Dict[str, Any]]) -> None:
    for record_dict in record_dict_list:
        attr_value: Any = attribute_calculators.ac(record_dict["data"])
        save_ac_value(record_dict["id"], attr_value)


async def process_llm_record_batch(record_dict_batch: List[Dict[str, Any]]) -> None:
    global DEFAULT_USER_PROMPT_A2VYBG

    for record_dict in record_dict_batch:
        attribute_calculators.USER_PROMPT_A2VYBG = prepare_and_render_mustache(
            DEFAULT_USER_PROMPT_A2VYBG, record_dict
        )

        attr_value: str = await attribute_calculators.ac(record_dict["data"])
        save_ac_value(record_dict["id"], attr_value)


async def process_async_llm_calls(record_dict_list: List[Dict[str, Any]]) -> None:
    global amount

    def make_batches(
        iterable: List[Any], size: int = 1
    ) -> Generator[List[Any], None, None]:
        length = len(iterable)
        for ndx in range(0, length, size):
            yield iterable[ndx : min(ndx + size, length)]

    batch_size = max(amount // int(attribute_calculators.NUM_WORKERS_A2VYBG), 1)
    tasks = [
        process_llm_record_batch(batch)
        for batch in make_batches(record_dict_list, size=batch_size)
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    _, iso2_code, payload_url, data_type = sys.argv

    print("Preparing data for attribute calculation.")

    # This import statement will always be highlighted as a potential error, as during devtime,
    # the script `labeling_functions` does not exist. It will be inserted at runtime
    import attribute_calculators

    DEFAULT_USER_PROMPT_A2VYBG = getattr(
        attribute_calculators, "USER_PROMPT_A2VYBG", None
    )

    vocab = spacy.blank(iso2_code).vocab

    with open("docbin_full.json", "r") as infile:
        docbin_data = json.load(infile)

    record_dict_list = parse_data_to_record_dict(docbin_data)

    py_data_types, check_data_type = get_check_data_type_function(data_type)

    print("Running attribute calculation.")
    calculated_attribute_by_record_id = {}
    amount = len(record_dict_list)
    progress_size = min(
        100,
        max(amount // int(getattr(attribute_calculators, "NUM_WORKERS_A2VYBG", 1)), 1),
    )
    processed_records = 0
    __print_progress(0.0)

    if data_type == "LLM_RESPONSE":
        asyncio.run(process_async_llm_calls(record_dict_list))
    else:
        process_attribute_calculation(record_dict_list)

    __print_progress(1.0)
    print("Finished execution.")
    requests.put(payload_url, json=calculated_attribute_by_record_id)
