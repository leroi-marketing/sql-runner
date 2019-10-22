from dialog import Dialog
import argparse
import json
import glob
import os
from subprocess import Popen

def main():
    parser = argparse.ArgumentParser(description='Interactive SQL runner.')

    parser.add_argument(
        'config',
        type=str,
        help='Path to the config file'
    )

    parser.add_argument(
        '--database',
        help='Database name',
        nargs='?',
        default=False
    )

    args = parser.parse_args()

    with open(args.config) as fp:
        config = json.load(fp)
        sqlpath = config['sql_path']
        del config

    d = Dialog(dialog="dialog")

    d.set_background_title("SQL Runner")

    code, action = d.menu("What do you want to do?",
                          choices=[('execute', 'Execute statements based on the provided list of CSV command files'),
                                   ('test', 'Test execution of statements based on the provided list of CSV command files'),
                                   ('staging', 'Executes commands as specified, but in staging schema'),
                                   ('deps', 'View dependencies graph'),
                                   ('clean', 'Schemata prefix to clean up')
                          ],
                          title="What do you want to do?",
                          backtitle="Interactive SQL Runner")
    
    if code != d.OK:
        Popen(["clear"]).wait()
        return

    cmd_args = []
    if args.database:
        cmd_args += ['--database', args.database]

    if action != 'deps':
        available_files = list((os.path.basename(f).split('.')[0], "", False) for f in glob.glob(sqlpath + "/*.csv"))
        available_files.sort()

        code, files = d.checklist("Press Space to select",
                                  choices=available_files,
                                  title="Which files to run?",
                                  backtitle="Interactive SQL Runner")
        if code != d.OK:
            Popen(["clear"]).wait()
            return

        if files:
            cmd_args += ['--' + action] + files
        else:
            Popen(["clear"]).wait()
            return
    else:
        cmd_args += ['--' + action]

    Popen(["clear"]).wait()

    args = ["runner", '--config', args.config] + cmd_args
    print(" ".join(args))
    proc = Popen(args)
    proc.wait()


if __name__ == '__main__':
    main()
