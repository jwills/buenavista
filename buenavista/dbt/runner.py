import importlib.util
import os
import tempfile

from pydantic import BaseModel

from buenavista.adapter import AdapterHandle


class DbtPythonJob(BaseModel):
    process_id: int
    module_name: str
    module_definition: str


def run_python_job(job: DbtPythonJob, handle: AdapterHandle, process_status: dict):
    mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
    mod_file.write(job.module_definition.lstrip().encode("utf-8"))
    mod_file.close()
    try:
        spec = importlib.util.spec_from_file_location(job.module_name, mod_file.name)
        if not spec:
            process_status[job.process_id] = {
                "ok": False,
                "status": "Failed to load python model as module",
            }
            return
        module = importlib.util.module_from_spec(spec)
        if spec.loader:
            spec.loader.exec_module(module)
        else:
            process_status[job.process_id] = {
                "ok": False,
                "status": "Python module spec is missing loader",
            }
            return

        # Do the actual work to run the code here
        process_status[job.process_id] = {"ok": True, "status": "Running"}
        dbt = module.dbtObj(handle.load_df_function)
        df = module.model(dbt, handle.cursor)
        module.materialize(df, handle.cursor)
        process_status[job.process_id] = {"ok": True, "status": "Success"}
    except Exception as err:
        process_status[job.process_id] = {"ok": False, "status": str(err)}
    finally:
        os.unlink(mod_file.name)
