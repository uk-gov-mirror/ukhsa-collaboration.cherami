import pytest

from cherami.pipelines.base import SampleQC
from cherami.pipelines.sarscov2 import SARSCoV2Pipeline


@pytest.fixture
def sarscov2_pipeline():
    return SARSCoV2Pipeline()


def test_sarscov2_pipeline_evaluate_sample(sarscov2_pipeline):
    sample_qc = SampleQC(
        sample_id="SAMPLE1",
        qc_pass=True,
        total_reads=100,
        spike_reads=10,
        taxon_reads={"sarscov2_reads": 1000},
        genus_percentage=1.0,
        species_reads=10,
    )
    assert sarscov2_pipeline.evaluate_sample(sample_qc) is True


def test_sarscov2_pipeline_generate_samplesheet(sarscov2_pipeline):
    ...
    ## TODO: test would go here for method
