check_functions = {}
ordered_check_function_names = []
function_categories = {}


def check_result(category: str):
    def check_result_wrapper(func):
        def runner():
            try:
                result = func()
                if result:
                    return {"result": "OK", "message": ""}
                else:
                    return {"result": "FAILED", "message": "Result is empty"}
            except Exception as exc:
                return {"result": "FAILED", "message": f"{str(exc)}"}

        if category not in check_functions:
            check_functions[category] = {}
        function_categories[func.__name__] = category
        check_functions[category][func.__name__] = runner
        ordered_check_function_names.append(func.__name__)
        return runner

    return check_result_wrapper
