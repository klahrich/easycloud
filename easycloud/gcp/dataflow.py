import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import pyaml


class Dataflow:

    def __init__(self, job_name, project, temp_location, input_table, output_table, output_schema, setup_file):
        self.options = {}
        self.options['job_name'] = job_name
        self.options['project'] = project
        self.options['temp_location'] = temp_location
        self.options['staging_location'] = temp_location
        self.options['runner'] = 'DataflowRunner'
        self.options['setup_file'] = setup_file
        self.input_table = project + ':' + input_table
        self.output_table = project + ':' + output_table
        self.output_schema = output_schema

    def run(self, runf_func):
        logging.getLogger().setLevel(logging.INFO)

        pipeline_options = PipelineOptions.from_dictionary(self.options)
        pipeline_options.view_as(SetupOptions).save_main_session = True

        with beam.Pipeline(options=pipeline_options) as p:
            p = (p | 'read_bq_table' >> beam.io.Read(beam.io.BigQuerySource(self.input_table)))
            p = runf_func(p)
            (p | 'write_bq_table' >> beam.io.gcp.bigquery.WriteToBigQuery(
                                        self.output_table,
                                        schema = self.output_schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('--config_file')

	args = parser.parse_args()

	with open(args.config_file) as f:
		config = yaml.load(f, Loader=yaml.FullLoader)

	flow = Dataflow(**config)
	flow.run()



