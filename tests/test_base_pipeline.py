from pathlib import Path

import pytest

from cherami.pipelines.base import Pipeline, PipelineConfig, PipelineCriteria, SampleQC


class DummyPipeline(Pipeline):
    def generate_samplesheet(self, samples, job_id):
        return

    def evaluate_sample(self, sample_qc: SampleQC) -> bool:
        return True


@pytest.fixture
def pipeline_config():
    work_dir = Path("work")
    output_dir = Path("outputs")
    return PipelineConfig(
        name="test",
        version="0.1.0",
        path="gpha/nf-pipeline",
        cpus=1,
        mem="4G",
        cpu_limit=1,
        mem_limit="4G",
        nf_config_path=Path("/configs/dummy.config"),
        nf_profiles=["test"],
        nf_extra_args=[],
        work_dir=work_dir,
        output_dir=output_dir,
        namespace="cherami",
        container="idont:exist",
    )


@pytest.fixture
def pipeline_criteria():
    return PipelineCriteria(
        min_taxon_reads=10,
        min_total_reads=100,
        require_spike=True,
        qc_pass=True,
        percentage_genus=0.5,
        min_total_species_reads=20,
    )


@pytest.fixture
def pipeline(pipeline_config, pipeline_criteria):
    return DummyPipeline(config=pipeline_config, criteria=pipeline_criteria)


@pytest.fixture
def pipeline_proc_names(pipeline_config, pipeline_criteria):
    proc_names = {"NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)": [0, 5]}
    return DummyPipeline(
        config=pipeline_config,
        criteria=pipeline_criteria,
        proc_names=proc_names,
    )


@pytest.fixture
def trace_file(tmp_path, content):
    trace_path = tmp_path / "trace.tsv"
    trace_path.write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\tduration\trealtime\t%cpu\tpeak_rss\tpeak_vmem\trchar\twchar\n"
        + content,
    )
    return trace_path


@pytest.mark.parametrize(
    "content",
    [
        "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_should_pass(pipeline, trace_file):
    assert pipeline.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t1\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_should_fail(pipeline, trace_file):
    assert pipeline.evaluate_exit_status(trace_file) is False


@pytest.mark.parametrize(
    "content",
    [
        "2\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t5\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n3\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_proc_names_should_pass(pipeline_proc_names, trace_file):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t1\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_proc_names_should_fail(pipeline_proc_names, trace_file):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
