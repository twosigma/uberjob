Lesson 5: Symbolic Tracebacks
=============================

When an exception occurs in a symbolic :meth:`~uberjob.Plan.call`, :func:`uberjob.run` provides three tracebacks (stack traces):

#. The traceback from the thread that called :func:`uberjob.run`.

#. The traceback from the failed execution of the symbolic call.

#. The traceback from when the symbolic call was originally made. This is known as the symbolic traceback. Symbolic tracebacks are useful when debugging.

Example
-------

Suppose that *example.py* contains the following program.

.. code-block:: ipython
   :linenos:

   import argparse
   import uberjob
   from uberjob.stores import JsonFileStore

   def parse_args():
       parser = argparse.ArgumentParser()
       parser.add_argument('--input-path', required=True)
       parser.add_argument('--output-path', required=True)
       return parser.parse_args()

   def fahrenheit_to_celcius(f):
       return (f - 32) * 5 / 9

   def build_plan(args):
       plan = uberjob.Plan()
       registry = uberjob.Registry()
       f = registry.source(plan, JsonFileStore(args.input_path))
       c = plan.call(fahrenheit_to_celcius, f)
       registry.add(c, JsonFileStore(args.output_path))
       return plan, registry

   def main():
       args = parse_args()
       plan, registry = build_plan(args)
       uberjob.run(plan, registry=registry)

   if __name__ == '__main__':
       main()

The program succeeds when given valid inputs.

.. code-block:: console

   echo 76 > input.json
   python example.py --input-path input.json --output-path output.json
   cat output.json
   24.444444444444443

When given an input which is not a number, the temperature conversion will fail.
The three tracebacks can be observed in the output text.

.. code-block:: console

   echo \"hello\" > input.json
   python example.py --input-path input.json --output-path output.json

   Traceback (most recent call last):
     File "/home/daniels/gitlab/uberjob/uberjob/execution/thread_pool.py", line 121, in process_node
       fn(node)
     File "/home/daniels/gitlab/uberjob/uberjob/execution/run_physical.py", line 76, in process
       retry(bound_call.value.run)()
     File "/home/daniels/gitlab/uberjob/uberjob/execution/run_physical.py", line 32, in run
       self.result.value = self.fn(*args, **kwargs)
     File "example.py", line 12, in fahrenheit_to_celcius
       return (f - 32) * 5 / 9
   TypeError: unsupported operand type(s) for -: 'str' and 'int'

   The above exception was the direct cause of the following exception:

   Traceback (most recent call last):
     File "example.py", line 28, in <module>
       main()
     File "example.py", line 25, in main
       uberjob.run(plan, registry=registry)
     File "/home/daniels/gitlab/uberjob/uberjob/run.py", line 195, in run
       raise CallError(e.node) from e.__cause__
   uberjob.errors.CallError: An exception was raised in a symbolic call to fahrenheit_to_celcius.
   Symbolic traceback (most recent call last):
     File "example.py", line 28, in <module>
     File "example.py", line 24, in main
     File "example.py", line 18, in build_plan


Symbolic tracebacks are also provided for symbolic calls added as a result of :meth:`uberjob.Registry.add` and
:meth:`uberjob.Registry.source`. This can be demonstrated by giving an invalid input path.

.. code-block:: console

   python example.py --input-path does_not_exist.json --output-path output.json

   Traceback (most recent call last):
     File "/home/daniels/gitlab/uberjob/uberjob/execution/thread_pool.py", line 121, in process_node
       fn(node)
     File "/home/daniels/gitlab/uberjob/uberjob/execution/run_physical.py", line 76, in process
       retry(bound_call.value.run)()
     File "/home/daniels/gitlab/uberjob/uberjob/execution/run_physical.py", line 32, in run
       self.result.value = self.fn(*args, **kwargs)
     File "/home/daniels/gitlab/uberjob/uberjob/stores/json_file_store.py", line 21, in read
       with open(self.path, encoding=self.encoding) as inputfile:
   FileNotFoundError: [Errno 2] No such file or directory: 'does_not_exist.json'

   The above exception was the direct cause of the following exception:

   Traceback (most recent call last):
     File "example.py", line 28, in <module>
       main()
     File "example.py", line 25, in main
       uberjob.run(plan, registry=registry)
     File "/home/daniels/gitlab/uberjob/uberjob/run.py", line 195, in run
       raise CallError(e.node) from e.__cause__
   uberjob.errors.CallError: An exception was raised in a symbolic call to uberjob.stores.json_file_store.JsonFileStore.read.
   Symbolic traceback (most recent call last):
     File "example.py", line 28, in <module>
     File "example.py", line 24, in main
     File "example.py", line 17, in build_plan

.. note::

   Symbolic tracebacks are implemented by preserving the call stack whenever a relevant symbolic operation is performed.
   They have a maximum depth to ensure that deep call stacks don't impact performance.
