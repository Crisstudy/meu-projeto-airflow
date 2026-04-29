from airflow.models import BaseOperator

class MaskCSVOperator(BaseOperator):
    
    def __init__(self, input_file: str, output_file: str, separator: str, columns: list, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.separator = separator
        self.columns = columns

    def execute(self, context):
        file = ''
        with open(self.input_file, 'r') as f:
            for date in f.readlines():
                fields = date.strip('\n').split(self.separator)
                fields[self.columns[0]] = '*******'
                file += self.separator.join(fields) + '\n'

            with open(self.output_file, 'w') as f:
                f.write(file)

        
       
       
        
     
        