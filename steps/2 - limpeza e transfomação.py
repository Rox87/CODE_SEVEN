import re
import pandas as pd

# Função para validar e-mails em uma pandas Series
def validate_clean_email_series(emails_series):
    # Regex para validação de e-mails
    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Aplicar a validação em toda a série usando .apply()
    return emails_series.apply(lambda email: email.lower().strip() if re.match(padrao, str(email)) else None)

def validate_clean_cpf_series(cpf_series):
    # Padronizar os CPFs: remover pontos, traços e validar que sejam números
    cpf_series = (
        cpf_series
        .astype(str)  # Garantir que os valores sejam strings
        .str.replace(r'\D', '', regex=True)  # Remover todos os caracteres não numéricos
        .str.strip()  # Remover espaços em branco
    )

    # Filtrar CPFs inválidos (com tamanho diferente de 11)
    cpf_series = cpf_series[cpf_series.str.len() == 11]

    return cpf_series


user_string_raw = StringIO(dict_files_contents['user_raw.csv'])
user_raw = pd.read_csv(user_string_raw,sep=",",encoding="utf-8")
user_raw.dropna(inplace=True)

# Padronizar o nome com as primeiras letras maiúsculas
user_raw['name'] = user_raw['name'].str.title()

# Padronizar os e-mails: letras minúsculas e remover espaços
user_raw['e-mail'] = validate_clean_email_series(user_raw['e-mail'])

# Padronizar e filtrar "cpf"
user_raw['cpf'] = validate_clean_cpf_series(user_raw['cpf'])

print(user_raw)