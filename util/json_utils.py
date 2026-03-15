
def get_value_by_path(data: dict, path: tuple[str, ...]):
    current = data

    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)

        if current is None:
            return None

    return current
    