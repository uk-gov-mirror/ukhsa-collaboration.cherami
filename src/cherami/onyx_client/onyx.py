# from dataclasses import dataclass

# from onyx import OnyxClient

# # config = OnyxConfig(
# #     domain=os.environ[OnyxEnv.DOMAIN],
# #     token=os.environ[OnyxEnv.TOKEN],
# # )


# @dataclass
# class OnyxQcResult:
#     qc_pass: bool
#     total_reads: int
#     spike_reads: int
#     genus_reads: int
#     genus_percentage: float
#     species_reads: int
#     species_percentage: float


# def query(sample_id: str):
#     with OnyxClient(config) as client:
#         record = client.get("synthscape", sample_id, include=["classifier_calls"])
#         # get taxon_id for sarscov2
#         taxon_id = 2901879
#         classcalls = record.get("classifier_calls", {})
#         sarscov2 = list(filter(lambda classcalls: classcalls["taxon_id"] == taxon_id, classcalls))
#         print(sarscov2)
#         # rets
#         # samples = list(
#         #     client.filter(
#         #         project="synthscape",
#         #         fields={"classifier_calls__taxon_id": 2697049},
#         #     )
#         # )
#         # print(samples[0])
