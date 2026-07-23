# One container image serves all four Lambda functions (LLD §7.1, D-2);
# Terraform overrides image_config.command per function (§10.5).
FROM public.ecr.aws/lambda/python:3.12

COPY pyproject.toml README.md ./
COPY ingestion ./ingestion
RUN pip install .

CMD ["ingestion.entrypoints.registrar_s3.handler"]
