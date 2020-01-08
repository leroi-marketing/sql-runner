from types import SimpleNamespace
from src.runner import main


def cloud_function(config, execute, test, staging, deps, clean, database):
    args_dict = {
        "config": config,
        "execute": execute,
        "test": test,
        "staging": staging,
        "deps": deps,
        "clean": clean,
        "database": database
    }

    args = SimpleNamespace(**args_dict)
    main(args)
