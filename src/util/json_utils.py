# def get_value_by_path(data: dict, path: tuple[str, ...]):
#     current = data

#     for key in path:
#         if not isinstance(current, dict):
#             return None
#         current = current.get(key)

#         if current is None:
#             return None

#     return current

def get_value_by_path(data, path):
    current = data

    for key in path:
        if isinstance(current, dict):
            if key not in current:
                return None
            current = current[key]

        elif isinstance(current, list):
            if not isinstance(key, int):
                return None
            if key < 0 or key >= len(current):
                return None
            current = current[key]

        else:
            return None

    return current