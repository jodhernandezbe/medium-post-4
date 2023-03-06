FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

USER root
RUN groupadd --g 1024 groupcontainer
RUN usermod -a -G groupcontainer glue_user

USER glue_user
WORKDIR /home/glue_user/workspace
COPY ./requirements.txt .

ENV PACKAGE="/home/glue_user/workspace/helpers"
ENV PYTHONPATH="${PYTHONPATH}:$PACKAGE"
ENV PATH="/home/glue_user/.local/bin:$PATH"

RUN python3 -m pip install --upgrade pip setuptools wheel --user
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt --user
