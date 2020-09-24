Lesson 4: Dependent Sources
===========================

Recall that :meth:`uberjob.Registry.source` can be used to create source nodes. Source nodes don't have
any dependencies when created, but they can be added using :meth:`uberjob.Plan.add_dependency`.
Such a node is known as a dependent source. It's not obvious at first, but they can be very useful.

Example
-------

Consider a scenario in which a subprocess must be called to transform a file.
For simplicity, this example uses the ``cp`` command. Real scenarios will of course be more complex.
An ideal solution will use two :class:`~uberjob.Registry` entries, one for the input and one for the output,
but what :class:`~uberjob.ValueStore` implementations should be used? Choosing the right value stores here is difficult,
and it's easy to come up something hacky such as an ``UnzipToJsonFileStore``.

The correct solution to this problem uses two key ideas. The first is to use :class:`~uberjob.stores.PathSource`,
a :class:`~uberjob.ValueStore` that returns its path when :meth:`~uberjob.stores.PathSource.read` is called and
raises :class:`NotImplementedError` when :meth:`~uberjob.stores.PathSource.write` is called.
The second is to use :meth:`~uberjob.Plan.add_dependency` to make the copy call a dependency of the output value store.
This encapsulates the side effect.


.. code-block:: ipython

   >>> ! echo 7 > a.json
   >>>
   >>> import subprocess
   >>> import uberjob
   >>> from uberjob.stores import PathSource, JsonFileStore
   >>>
   >>> plan = uberjob.Plan()
   >>> registry = uberjob.Registry()
   >>>
   >>> a_path = registry.source(plan, PathSource('a.json'))
   >>> b_path = 'b.json'
   >>> copy_call = plan.call(subprocess.check_call, ['cp', a_path, b_path])
   >>> b = registry.source(plan, JsonFileStore(b_path))
   >>> plan.add_dependency(copy_call, b)
   >>>
   >>> b2 = plan.call(pow, b, 2)
   >>>
   >>> uberjob.render(plan, registry=registry)
   >>> uberjob.run(plan, registry=registry, output=b2)
   49

.. raw:: html

    <object data="_static/lesson4_1.svg" type="image/svg+xml"></object>

Simple experiments involving updating the input file or deleting the output file will confirm that the subprocess call
happens exactly when necessary.
