Contexto de Negócios e Perguntas (Etapa 2. e 4.1)

Contexto e Estrutura dos Dados Brutos:
O agronegócio é um setor guiado por precisão técnica. Este projeto visa analisar dados de agricultura de precisão para entender as condições ideais de clima e solo exigidas por diferentes culturas. Os dados brutos foram 
obtidos através do Kaggle (dataset "Crop Recommendation Dataset"). A estrutura original é composta por um arquivo CSV contendo atributos numéricos de solo (N - Nitrogênio, P - Fósforo, K - Potássio, além do pH) e 
atributos climáticos (temperatura, umidade e pluviosidade/chuva), finalizando com uma variável categórica indicando a cultura recomendada para aquelas condições.

Licença: Os dados possuem licença de dados abertos, o que permite sua utilização livre para fins acadêmicos.

Perguntas de Negócio:

1.Quais as faixas médias de temperatura e pluviosidade para o Algodão comparado ao Café?

2.Quais culturas exigem os maiores níveis de Nitrogênio no solo?

3.Solos com pH extremamente ácido (abaixo de 5.5) limitam o plantio a quais culturas?

4.Quais as melhores frutas para o clima tropical (Temperatura > 25°C e Chuva > 100mm)?

5.Qual cultura exige o maior custo de investimento em fertilizantes ?

6.Em um cenário de crise hídrica (chuva menor que 60mm), quais culturas são as mais seguras para investimento?

Carga dos Dados (Etapa 4.2)

A carga inicial de dados consistiu no download manual do arquivo .csv a partir do repositório público do Kaggle. A passagem para o ambiente de nuvem foi realizada pela interface visual da plataforma databricks. 
O arquivo bruto foi carregado para o armazenamento DBFS por upload manual.
<img width="1904" height="980" alt="image" src="https://github.com/user-attachments/assets/ee8a0d0c-0116-4069-8fee-ef24062e4029" />

Modelagem e Catálogo de Dados (Etapa 4.3)

Na camada Gold, escolhi usar o modelo flat. Como no pipeline eu queria extrair o perfil ideal de cada cultura para ver a melhor tomada de decisão, não foi necessário utilizar um esquema de modelagem mais complexo. A tabela
final já tem todas as métricas necessárias em uma única visão otimizada para consultas analíticas.

Catálogo de Dados (Tabela: agricultura_gold_resumo):

cultura (String): Nome da cultura agrícola traduzido para o português.

media_nitrogenio, media_fosforo, media_potassio (Inteiros): Média ideal da proporção do macronutriente no solo.

media_temperatura (Decimal 18,8): Temperatura média ideal suportada em °C.

media_chuva (Decimal 18,8): Volume médio de pluviosidade exigida em mm.

ph_minimo, ph_maximo (Decimal 18,8): Faixa de tolerância de acidez/alcalinidade do solo.
<img width="1276" height="676" alt="image" src="https://github.com/user-attachments/assets/9e938ef3-b55c-4082-9cd6-c31ddbce6658" />

Pipeline de Dados (Etapa 4.4)

O processo de ETL foi todo feito num único notebook no databricks, usei a linguagem PySpark para as transformações lógicas, seguindo a arquitetura de Medalhão:

Bronze: Ingestão do dado original garantindo a rastreabilidade.

Silver: Limpeza, tradução, formatação e aplicação de regras de negócio (remoção de culturas fora do escopo). Os dados tratados foram persistidos em formato Delta (agricultura_silver).

Gold: junção dos dados por cultura, gerando médias, máximos e mínimos para responder às perguntas feitas inicialmente.

TABELA BRONZE
<img width="1341" height="727" alt="image" src="https://github.com/user-attachments/assets/fc5dbef0-b088-43a2-8e38-e8c2abae728a" />
TABELA PRATA 
<img width="1276" height="506" alt="image" src="https://github.com/user-attachments/assets/17df7edd-aaa0-4905-896d-7ec85d7c917c" />
TABELA OURO
<img width="1342" height="610" alt="image" src="https://github.com/user-attachments/assets/c350cc16-b77a-4eef-8873-b8d666164f40" />

Qualidade de Dados (Etapa 4.5)

Durante a passagem da camada bronze para prata, fiz o perfil dos dados, identifiquei e corrigi os seguintes pontos:

Completude e Unicidade: O dataset base possuía 100% de completude. Contudo, aplicou-se .dropna() e .dropDuplicates() no PySpark como garantia do pipeline contra futuros dados corrompidas.

Consistência: Os rótulos das culturas foram traduzidos para português via dicionário (.replace()) para padronizar o idioma.

Padronização e Acurácia: identifiquei uma poluição visual no número de casas decimais. Apliquei o método .cast("decimal(18,8)") nas variáveis contínuas. As variáveis de solo deixei como inteiros pois já vieram assim.

Outliers e Escopo: Observwi a presença de outliers climáticos (volume de chuva alto). Decidi manter, pois representam condições climáticas reais como as monções para o arroz e não erros. Culturas 
irrelevantes para o mercado alvo (ex: pigeonpeas, jute) foram removidas do dataset via filtro (.filter).


Análise de Dados (Etapa 4.5)

Com a tabela Gold pronta, respondi as perguntas por consultas SQL:

Pergunta 1- Quais as faixas médias de temperatura e chuva para Algodão comparado ao Café?
<img width="1384" height="511" alt="image" src="https://github.com/user-attachments/assets/a935cef8-ebb9-4717-9ea9-aec97542bb57" />

A análise mostra que o café exige um volume hídrico maior, mais ou menos 78 mm, comparado ao algodão. Enquanto temperatura média é parecida sendo em torno de 1.5°C a diferenca entre elas.

Pergunta 2- Quais culturas exigem, em média, os maiores níveis de Nitrogênio no solo?
<img width="1349" height="539" alt="image" src="https://github.com/user-attachments/assets/784345d4-e31b-4907-890a-554af4b58020" />

As três culturas que mais necessitam de Nitrogênio são algodão, café, banana, melão e melancia, ou seja, o nitrogênio é prioridade para o desenvolvimento dessas plantas.

Pergunta 3- Solos com pH extremamente ácido (abaixo de 5.5) limitam o plantio a quais tipos de culturas?
<img width="1346" height="560" alt="image" src="https://github.com/user-attachments/assets/633cc8e3-1372-41a6-9dc4-3ce12b54a9bd" />

Esse nivel de acidez exige culturas muito resistentes e na tabela temos a manga e o arroz que cumprem esse requisito.

Pergunta 4- Quais são as melhores frutas para cultivar no clima tropical? 
<img width="1368" height="626" alt="image" src="https://github.com/user-attachments/assets/de723a2d-1659-4456-9cfb-b425d0508fcc" />

Filtrando temperatura > 25°C e chuva > 100mm, os 3 melhores plantios para essa região são de mamão, coco e banana, indicando ótima adaptação ao estresse térmico e hídrico.

Pergunta 5-Qual cultura exige o maior investimento financeiro em adubação? 
<img width="1402" height="553" alt="image" src="https://github.com/user-attachments/assets/21acdbf4-4cc8-468f-98aa-5b8e71b819c3" />

Somando a necessidade de N, P e K, a cultura que precisa do maior investimento em correção de solo são uva, macã e banana mostrando um alto custo de manutenção.

Pergunta 6- Em um cenário de crise hídrica ou em regiões áridas, quais são as culturas mais seguras para se investir (aquelas que sobrevivem com menos de 60 mm de chuva)?
<img width="1388" height="549" alt="image" src="https://github.com/user-attachments/assets/10b6d9ed-960d-42f9-990b-3eb66e83c37f" />

Em locais de seca culturas como o melão, melancia e lentilha tem maior resiliência hídrica e conseguem de manter em zonas de segurança.


Autoavaliação

Ao finalizar este trabalho, considero que consegui cumprir meus objetivos iniciais e responder satisfatoriamente as perguntas. O fluxo ponta a ponta funcionou bem,transformando um dado bruto em inteligência de negócio
pronta para ser utilizada.
A maior dificuldade que encontrei durante a execução foi estruturar o raciocínio em camadas na arquitetura medalhão, entender exatamente como deveria manipular os dados para que conseguisse responder corretamente as perguntas
ao final e quais dados não eram relevantes, entendendo exatamente o que deveria ser feito em cada camada sem correr o risco de misturar.
Como trabalhos futuros, pretendo evoluir este projeto adicionando a ingestão automatizada via API de dados mercadológicos e climáticos para cruzar a viabilidade climática por região com o lucro estimado por safra.



