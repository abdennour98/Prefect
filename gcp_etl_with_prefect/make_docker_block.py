from prefect.infrastracture.docker import DockerContainer
docker_block=DockerContainer(
    image="elmalki1998/prefect:zoomcamp",
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("zoomcamp",overwrite=True)