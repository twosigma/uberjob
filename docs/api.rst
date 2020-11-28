.. index::
   single: API reference
.. _api-reference:

API Reference
=============

uberjob.Plan
------------

.. autoclass:: uberjob.Plan
   :members:


uberjob.run
-----------

.. autofunction:: uberjob.run


uberjob.render
--------------

.. autofunction:: uberjob.render


uberjob.Registry
----------------

.. autoclass:: uberjob.Registry
   :members:

   .. automethod:: __contains__

   .. automethod:: __getitem__

   .. automethod:: __iter__

   .. automethod:: __len__


uberjob.ValueStore
------------------

.. autoclass:: uberjob.ValueStore
   :members:


uberjob.stores
--------------

.. automodule:: uberjob.stores
   :members:
   :inherited-members:


uberjob.graph
-------------

.. automodule:: uberjob.graph
   :members:


uberjob.progress
----------------

.. automodule:: uberjob.progress

.. autoclass:: uberjob.progress.Progress
   :members:

.. autodata:: uberjob.progress.default_progress

.. autodata:: uberjob.progress.console_progress

.. autodata:: uberjob.progress.ipython_progress

.. autodata:: uberjob.progress.null_progress

.. autofunction:: uberjob.progress.html_progress

.. autofunction:: uberjob.progress.composite_progress

.. autoclass:: uberjob.progress.ProgressObserver
   :members:

   .. automethod:: __enter__

   .. automethod:: __exit__


Exceptions
----------

.. autoclass:: uberjob.CallError
   :members:

.. autoclass:: uberjob.NotTransformedError
   :members:
