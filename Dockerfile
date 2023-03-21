FROM python:3.10.10
RUN poetry init
ADD pipeline.py /
CMD [ "python", "./pipeline.py" ]