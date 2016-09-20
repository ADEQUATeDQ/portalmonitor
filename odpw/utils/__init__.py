
def extras_to_dicts(datasets):
    for dataset in datasets:
        extras_to_dict(dataset)


def extras_to_dict(dataset):
    extras_dict = {}
    if dataset and isinstance(dataset, dict):
        extras = dataset.get("extras", {})
        if isinstance(extras, list):
            for extra in extras:
                if isinstance(extra, dict) and 'key' in extra and 'value' in extra:
                    key = extra["key"]
                    value = extra["value"]
                    extras_dict[key] = value
            dataset["extras"] = extras_dict
