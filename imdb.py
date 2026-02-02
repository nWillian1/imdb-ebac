import requests # Importação da biblioteca requests para fazer requisições HTTP
import time # Importação da biblioteca time para manipulação de tempo
import csv # Importação da biblioteca csv para manipulação de arquivos CSV
import random # Importação da biblioteca random para gerar números aleatórios
import concurrent.futures # Importação da biblioteca concurrent.futures para execução paralela
from bs4 import BeautifulSoup # Importação da biblioteca BeautifulSoup para análise de HTML

# global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'} # Definindo o cabeçalho do agente de usuário para as requisições HTTP

MAX_THREADS = 7 # Definindo o número máximo de threads para execução paralela

def extract_movie_details(movie_link): # Função para extrair detalhes de um filme a partir do link fornecido
    time.sleep(random.uniform(0, 0.2)) # Pausa aleatória entre 0 e 0.2 segundos para evitar sobrecarga no servidor
    response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser') # Fazendo a requisição HTTP e analisando o conteúdo HTML com BeautifulSoup
    movie_soup = response # Obtendo o conteúdo HTML da página do filme

    if movie_soup is not None: # Verificando se o conteúdo HTML foi obtido com sucesso
        title = None # Inicializando a variável title como None
        date = None # Inicializando a variável date como None
        
        # Encontrando a seção específica
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'}) # Encontrando a seção da página com a classe 'ipc-page-section'
        
        if page_section is not None: # Verificando se a seção foi encontrada com sucesso
            # Encontrando todas as divs dentro da seção
            divs = page_section.find_all('div', recursive=False) # Encontrando todas as divs diretas dentro da seção
            
            if len(divs) > 1: # Verificando se há mais de uma div encontrada
                target_div = divs[1] # Selecionando a segunda div como a div alvo
                
                # Encontrando o título do filme
                title_tag = target_div.find('h1') # Encontrando a tag h1 que contém o título do filme
                if title_tag: # Verificando se a tag h1 foi encontrada com sucesso
                    title = title_tag.find('span').get_text() # Extraindo o texto do título do filme
                
                # Encontrando a data de lançamento
                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)   # Encontrando a tag <a> que contém a data de lançamento
                if date_tag: # Verificando se a tag <a> foi encontrada com sucesso
                    date = date_tag.get_text().strip() # Extraindo o texto da data de lançamento
                
                # Encontrando a classificação do filme
                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'}) # Encontrando a tag que contém a classificação do filme
                rating = rating_tag.get_text() if rating_tag else None # Extraindo o texto da classificação do filme
                
                # Encontrando a sinopse do filme
                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'}) # Encontrando a tag que contém a sinopse do filme
                plot_text = plot_tag.get_text().strip() if plot_tag else None # Extraindo o texto da sinopse do filme
                
                with open('movies.csv', mode='a', newline='', encoding='utf-8-sig') as file: # Abrindo o arquivo CSV para escrita
                    movie_writer = csv.writer(file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL) # Criando um escritor CSV
                    if all([title, date, rating, plot_text]): # Verificando se todas as informações foram extraídas com sucesso
                        print(title, date, rating, plot_text) # Imprimindo as informações do filme
                        movie_writer.writerow([title, date, rating, plot_text]) # Escrevendo as informações do filme no arquivo CSV

def extract_movies(soup): # Função para extrair a lista de filmes da página principal
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul') # Encontrando a tabela de filmes na página principal
    movies_table_rows = movies_table.find_all('li') # Encontrando todas as linhas da tabela de filmes
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows] # Extraindo os links dos filmes

    threads = min(MAX_THREADS, len(movie_links))    # Definindo o número de threads a serem usadas, limitado pelo número máximo definido
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor: # Criando um pool de threads para execução paralela
        executor.map(extract_movie_details, movie_links) # Mapeando a função de extração de detalhes do filme para os links dos filmes

def main(): # Função principal do programa
    start_time = time.time() # Registrando o tempo de início da execução

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm' # URL da página de filmes mais populares do IMDB
    response = requests.get(popular_movies_url, headers=headers) # Fazendo a requisição HTTP para a página de filmes mais populares
    soup = BeautifulSoup(response.content, 'html.parser') # Analisando o conteúdo HTML da página com BeautifulSoup

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup) # Chamando a função para extrair os filmes da página principal

    end_time = time.time() # Registrando o tempo de término da execução
    print('Total time taken: ', end_time - start_time) # Imprimindo o tempo total de execução

if __name__ == '__main__': # Verificando se o script está sendo executado diretamente
    main() # Chamando a função principal do programa