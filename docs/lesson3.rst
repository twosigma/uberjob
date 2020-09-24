Lesson 3: Gather
================

The gather operation is often used implicitly and unconsciously, but its details are worth understanding.
:meth:`~uberjob.Plan.gather` converts non-symbolic values to symbolic values.
For example, it will convert an integer to an integer literal, and it will convert a list of symbolic nodes into a
symbolic node whose value is a list.
To achieve this latter behavior, it must recursively search structured values for symbolic nodes.
When doing this, it only recognizes Python's general purpose built-in containers:
:class:`dict`, :class:`list`, :class:`set`, and :class:`tuple`.

:meth:`~uberjob.Plan.call` automatically gathers its arguments, and :func:`uberjob.run` automatically gathers its output.

Example
-------

Implicit gathering. This is the preferred way. Note that literals are created explicitly here
for illustration purposes; implicit gathering is the preferred way to create them too.

.. code-block:: ipython

   >>> import uberjob
   >>> plan = uberjob.Plan()
   >>> x = plan.lit(1)
   >>> y = plan.lit(2)
   >>> z = plan.call(sum, [x, y])
   >>> uberjob.run(plan, output=z)
   3
   >>> uberjob.render(plan)

.. raw:: html

    <object data="_static/lesson3_1.svg" type="image/svg+xml"></object>

More complex structured values can be gathered as well:

.. code-block:: ipython

   >>> import json
   >>> plan = uberjob.Plan()
   >>> x = plan.lit(1)
   >>> y = plan.lit(2)
   >>> z = plan.call(json.dumps, {'x': x, 'y': y, 'w': [x, (y, y)]})
   >>> uberjob.run(plan, output=z)
   '{"x": 1, "y": 2, "w": [1, [2, 2]]}'
   >>> uberjob.render(plan)

.. raw:: html

    <object data="_static/lesson3_2.svg" type="image/svg+xml"></object>

The :code:`output` argument of :func:`~uberjob.run` is also implicitly gathered:

.. code-block:: ipython

   >>> plan = uberjob.Plan()
   >>> x = plan.lit(1)
   >>> y = plan.lit(2)
   >>> uberjob.run(plan, output=[x, y])
   [1, 2]


Gather can be called explicitly, but this is usually unnecessary:

.. code-block:: ipython

   >>> plan = uberjob.Plan()
   >>> x = plan.lit(1)
   >>> y = plan.lit(2)
   >>> w = plan.gather([x, y])
   >>> z = plan.call(sum, w)
   >>> uberjob.run(plan, output=z)
   3

.. warning::
   `gather` will raise a stack overflow exception if passed a structured value that contains a cycle, such as a list that contains itself.
   This will also happen with `call` because it gathers all of its arguments.
   To resolve this, explicitly convert the offending value to a literal using `lit`.
