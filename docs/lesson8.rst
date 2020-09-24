Lesson 8: Forcing Rebuilds
==========================

Sometimes manual action is required to correctly rebuild outputs. Code, configuration, and environment changes can
make this necessary. The easiest way to force outputs to be recomputed is to manually delete them,
keeping in mind that dependents of deleted outputs will be rebuilt automatically.
This is the preferred solution when only some of the outputs are stale and it would be expensive to recompute everything.

There is also a way to reliably recompute everything. When :func:`uberjob.run` is provided the ``fresh_time`` argument,
stored values whose modified time is older than the fresh time will be considered stale.
The fresh time is usually persisted using an empty file called the "fresh file", which ensures that rebuilds which
fail partway through continue correctly upon rerunning.
Another option is to pass :meth:`datetime.datetime.now`; this can be useful during development.

Example
-------

Consider the following program, which is reused from a previous lesson with some modifications.
The touch file is updated when the ``--fresh`` command-line argument is present,
and its file interactions are conveniently handled using :class:`~uberjob.stores.TouchFileStore`.

.. code-block:: ipython

    import argparse
    import os
    import uberjob
    from uberjob.stores import JsonFileStore, TouchFileStore

    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('--input-directory', required=True)
        parser.add_argument('--output-directory', required=True)
        parser.add_argument('--fresh', action='store_true')
        return parser.parse_args()

    def fahrenheit_to_celcius(f):
        return (f - 32) * 5 / 9

    def build_plan(args):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        f = registry.source(plan,
                            JsonFileStore(os.path.join(args.input_directory, 'input.json')))
        c = plan.call(fahrenheit_to_celcius, f)
        registry.add(c, JsonFileStore(os.path.join(args.output_directory, 'output.json')))
        return plan, registry

    def main():
        args = parse_args()
        os.makedirs(args.output_directory, exist_ok=True)
        touch_store = TouchFileStore(os.path.join(args.output_directory, '_FRESH'))
        if args.fresh:
            touch_store.write(None)
        plan, registry = build_plan(args)
        uberjob.run(plan, registry=registry, fresh_time=touch_store.get_modified_time())

    if __name__ == '__main__':
        main()
