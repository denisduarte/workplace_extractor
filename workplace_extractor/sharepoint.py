from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.files.file import File
from io import BytesIO
import pandas as pd


class SharepointConnector:
    def __init__(self, url, site, folder, client_id, client_secret):
        self.url = url
        self.site = site
        self.folder = folder
        self.client_id = client_id
        self.client_secret = client_secret

        try:
            self.get_context()
        except ValueError as e:
            print(f'Credenciais do Sharepoint expiradas. Client ID = {client_id}')
            raise e

    def get_context(self):
        site_url = self.url + self.site

        ctx_auth = AuthenticationContext(site_url)
        ctx_auth.acquire_token_for_app(self.client_id, self.client_secret)

        return ClientContext(site_url, ctx_auth)

    def upload_file(self, content, file_name, file_path=''):
        ctx = self.get_context()

        file = ctx.web.get_folder_by_server_relative_url(self.site + self.folder + file_path)\
                      .upload_file(file_name, content)\
                      .execute_query()

        return file

    def upload_from_df(self, df, file_name, file_path, file_type='CSV'):
        df = df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True)

        s_buf = BytesIO()
        if file_type == 'CSV':
            df.to_csv(s_buf, index=False, sep=";", encoding="iso-8859-1")
        if file_type == 'Excel':
            df.to_excel(s_buf, index=False, encoding="iso-8859-1")

        s_buf.seek(0)
        self.upload_file(s_buf, file_name, file_path)

    def download_file(self, file_name, file_path=''):
        ctx = self.get_context()

        file_url = self.site + self.folder + file_path + file_name
        file_content = File.open_binary(ctx, file_url).content

        return file_content

    def download_to_df(self, file_name, file_path, file_type='CSV'):

        file = self.download_file(file_name, file_path)

        if file_type == 'CSV':
            try:
                try:
                    return pd.read_csv(BytesIO(file), sep=";", encoding='utf-8')
                except UnicodeDecodeError:
                    return pd.read_csv(BytesIO(file), sep=";", encoding='iso-8859-1')
            except pd.errors.ParserError:
                try:
                    return pd.read_csv(BytesIO(file), sep=",", encoding='utf-8')
                except UnicodeDecodeError:
                    return pd.read_csv(BytesIO(file), sep=",", encoding='iso-8859-1')
        if file_type == 'Excel':
            return pd.read_excel(BytesIO(file))
