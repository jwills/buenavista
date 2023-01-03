import importlib.util
import os
import tempfile

from .helpers import SimpleQueryResult
from buenavista.adapter import AdapterHandle, Extension, QueryResult


class DbtPythonRunner(Extension):
    def type(self) -> str:
        return "dbt_python_job"

    def apply(self, job: dict, handle: AdapterHandle) -> QueryResult:
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(job["module_definition"].lstrip().encode("utf-8"))
        mod_file.close()
        try:
            spec = importlib.util.spec_from_file_location(
                job["module_name"],
                mod_file.name,
            )
            if not spec:
                raise Exception("Failed to load python model as module")
            module = importlib.util.module_from_spec(spec)
            if spec.loader:
                spec.loader.exec_module(module)
            else:
                raise Exception("Module spec did not include a loader")
            # Do the actual work to run the code here
            cursor = handle.cursor()
            dbt = module.dbtObj(handle.load_df_function)
            df = module.model(dbt, cursor)
            module.materialize(df, cursor)
            return SimpleQueryResult("msg", "Success")
        finally:
            os.unlink(mod_file.name)
