from itertools import chain, starmap


def json_flattener(dictionary):
    """Flatten a nested json file"""

    def unpack(parent_key, parent_value):
        """Unpack one level of nesting in json file"""
        # Unpack one level only!!!

        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = parent_key + '.' + key
                yield temp1, value
        elif isinstance(parent_value, list):
            for idx, value in enumerate(parent_value):
                temp2 = parent_key + '.' + str(idx)
                yield temp2, value
        else:
            yield parent_key, parent_value

    # Keep iterating until the termination condition is satisfied
    while True:
        # Keep unpacking the json file until all values are atomic elements
        # (not dictionary or list)
        dictionary = dict(
            chain.from_iterable(starmap(unpack, dictionary.items()))
        )

        # Terminate condition: not any value in json file dictionary or list
        if not any(
                isinstance(value, dict) for value in dictionary.values()
            ) \
                and not any(
                    isinstance(value, list) for value in dictionary.values()
                ):
            break

    return dictionary
