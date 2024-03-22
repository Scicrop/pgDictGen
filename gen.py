import psycopg2
import sshtunnel
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from decouple import config

def generate_data_dictionary(ssh_host, ssh_port, ssh_username, ssh_private_key_path, postgres_host, postgres_port,
                             postgres_dbname, postgres_user, postgres_password, schemas):
    # Configuração do túnel SSH
    with sshtunnel.SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_private_key=ssh_private_key_path,
            remote_bind_address=(postgres_host, postgres_port)
    ) as tunnel:

        conn = psycopg2.connect(
            user=postgres_user,
            password=postgres_password,
            host="localhost",
            port=tunnel.local_bind_port,
            database=postgres_dbname
        )
        cursor = conn.cursor()
        data_dictionary = {}

        for schema in schemas:
            cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
            """)
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                table_info = {}
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    AND table_schema = '{schema}'
                """)
                columns = cursor.fetchall()
                table_info['columns'] = {}
                for column in columns:
                    column_name, data_type, is_nullable, column_default = column
                    table_info['columns'][column_name] = {
                        'data_type': data_type,
                        'is_nullable': is_nullable,
                        'default_value': column_default
                    }
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_name = '{table_name}'
                    AND constraint_name LIKE '%_pkey'
                    AND table_schema = '{schema}'
                """)
                primary_keys = cursor.fetchall()
                table_info['primary_keys'] = [pk[0] for pk in primary_keys]
                data_dictionary[f"{schema}.{table_name}"] = table_info


        cursor.close()
        conn.close()

        return data_dictionary


def generate_pdf(data_dictionary, output_file):
    styles = getSampleStyleSheet()
    table_style = TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                              ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                              ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                              ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                              ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                              ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                              ('GRID', (0, 0), (-1, -1), 1, colors.black)])

    elements = []

    for table_name, table_info in data_dictionary.items():
        data = [[f"Tabela: {table_name}"]]
        for column_name, column_info in table_info['columns'].items():
            data.append([
                            f"Coluna: {column_name}, Tipo de Dados: {column_info['data_type']}, Nulo: {column_info['is_nullable']}, Valor Padrão: {column_info['default_value']}"])

        table = Table(data)
        table.setStyle(table_style)
        elements.append(table)
        elements.append(Paragraph("<br/><br/>", styles["Normal"]))

    doc = SimpleDocTemplate(output_file, pagesize=letter, rightMargin=0.75 * inch, leftMargin=0.75 * inch,
                            topMargin=0.75 * inch, bottomMargin=0.75 * inch)


    def add_header(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 12)
        canvas.drawString(50, 750, "Data Dictionary")
        canvas.line(50, 745, 600, 745)
        canvas.restoreState()

    def add_footer(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 9)
        page_num = canvas.getPageNumber()
        canvas.drawString(50, 30, f"Page {page_num}")
        canvas.restoreState()

    def add_header_and_footer(canvas, doc):
        add_header(canvas, doc)
        add_footer(canvas, doc)

    doc.build(elements, onFirstPage=add_header_and_footer, onLaterPages=add_header_and_footer)


def main():

    SSH_HOST = config('SSH_HOST')
    SSH_PORT = config('SSH_PORT', cast=int)
    SSH_USERNAME = config('SSH_USERNAME')
    SSH_PRIVATE_KEY_PATH = config('SSH_PRIVATE_KEY_PATH')

    POSTGRES_HOST = config('POSTGRES_HOST')
    POSTGRES_PORT = config('POSTGRES_PORT', cast=int)
    POSTGRES_DBNAME = config('POSTGRES_DBNAME')
    POSTGRES_USER = config('POSTGRES_USER')
    POSTGRES_PASSWORD = config('POSTGRES_PASSWORD')

    schemas = ['public', 'outro_esquema']

    data_dictionary = generate_data_dictionary(
        SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PRIVATE_KEY_PATH,
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PASSWORD, schemas
    )

    output_file = 'data_dictionary.pdf'
    generate_pdf(data_dictionary, output_file)

if __name__ == "__main__":
    main()
