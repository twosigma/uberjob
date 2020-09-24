Lesson 1: The Plan
==================

The :class:`~uberjob.Plan` is the core abstraction in überjob.
It represents a symbolic call graph (not an instance of an execution).
As a result, it can be inspected, it can be run multiple times, and it can be transformed in various ways.
It's similar to a SQL query in this regard.

Example 1
---------

Symbolic calls are made with :meth:`~uberjob.Plan.call`, and symbolic calls can be inputs to other symbolic calls.
:meth:`~uberjob.Plan.call` will raise an exception if the provided arguments can't bind with the function.
Note that its signature is similar to :meth:`concurrent.futures.Executor.submit`.


A Plan can be run using :func:`uberjob.run`, which will only do what's necessary to compute the requested output.
It won't do anything here if ``output`` is not provided; this concept is known as pruning.

.. code-block:: ipython

   >>> import operator
   >>> import uberjob
   >>>
   >>> def area(width, height):
   ...     return width * height
   ...
   >>> plan = uberjob.Plan()
   >>> x = plan.call(operator.add, 1, 2)
   >>> y = plan.call(operator.add, 3, 4)
   >>> z = plan.call(area, width=x, height=y)
   >>> uberjob.run(plan, output=z)
   21

The :func:`~uberjob.run` call above also displays the following progress widget.
Jupyter Notebook, HTML, and console progress options are available in :mod:`uberjob.progress` and can be specified using the ``progress`` argument.
Multiple can be specified at once.
When not running in Jupyter, the console progress is used by default.

.. raw:: html

    <img src="_static/lesson1_ipython_progress.png"></img>

A Plan can be rendered using :func:`uberjob.render`.
The render function is a great tool for understanding how überjob works.

.. code-block:: ipython

   >>> uberjob.render(plan)

.. raw:: html

    <object data="_static/lesson1_1.svg" type="image/svg+xml"></object>

:meth:`~uberjob.Plan.add_dependency` can be used to add non-argument dependencies.
This is useful when side effects are involved.
Note that :func:`uberjob.run` will raise an exception if the :class:`~uberjob.Plan` has a cycle.

.. code-block:: ipython

   >>> plan.add_dependency(x, y)
   >>> uberjob.render(plan)

.. raw:: html

    <object data="_static/lesson1_2.svg" type="image/svg+xml"></object>

Example 2
---------

Non-symbolic arguments passed to :meth:`~uberjob.Plan.call` are automatically made symbolic using :meth:`~uberjob.Plan.gather`, which is covered in a later lesson.
For now, assume they are converted to symbolic literals.
Literals can be created explicitly using :meth:`~uberjob.Plan.lit`.
The return type of :meth:`~uberjob.Plan.call` is :class:`~uberjob.graph.Call` and the return type of :meth:`~uberjob.Plan.lit` is :class:`~uberjob.graph.Literal`.
Instances of both of these classes are called nodes.
Nodes are simple classes and their contents are easily inspected.
A pair of nodes can have multiple edges between them, making the underlying data structure a multidigraph.
The actual `networkx <https://networkx.github.io/>`_ graph can be accessed using :attr:`uberjob.Plan.graph`.

.. code-block:: ipython

   >>> plan = uberjob.Plan()
   >>> x = plan.lit(7)
   >>> y = plan.call('{}{}{a}{b}'.format, x, x, a=x, b=x)
   >>> uberjob.run(plan, output=y)
   '7777'
   >>> type(x)
   uberjob.graph.Literal
   >>> x.value
   7
   >>> type(y)
   uberjob.graph.Call
   >>> y.fn(1, 2, a=3, b=4)
   '1234'
   >>> uberjob.render(plan)

.. raw:: html

    <object data="_static/lesson1_3.svg" type="image/svg+xml"></object>


Recap
-----

To summarize, the :class:`~uberjob.Plan` is used to build a symbolic call graph which has two node types and three edge types.

- Node types

  - :class:`~uberjob.graph.Call`
  - :class:`~uberjob.graph.Literal`

- Edge types

  - :class:`~uberjob.graph.Dependency`
  - :class:`~uberjob.graph.PositionalArg`
  - :class:`~uberjob.graph.KeywordArg`
