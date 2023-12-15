from neo4j import GraphDatabase
import csv



# Conexão Neo4j
uri = "bolt://localhost:7687"  
username = "neo4j"  
password = "senacsenac"  





#################EXERCICIO 1 - Doador doou para Recebedor


# Import dados do CSV para o Neo4j
def exercicio1(uri, username, password, csv_file):
    driver = GraphDatabase.driver(uri, auth=(username, password))


    with driver.session() as session:
        session.write_transaction(delete_all_nodes_and_relationships)    


    print("Inserindo dados do CSV - Exercicio 01")
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        #next(csv_reader)  # Pule o cabeçalho

        for row in csv_reader:
            doador, cnpj, _, _, data, protocolo, valor, _, _, nome, _, _, partido, pais, origem = row
            # Crie um nodo para o doador
            with driver.session() as session:
                session.write_transaction(create_doador_ex1, doador, cnpj)
            # Crie um nodo para o recebedor (partido)
            with driver.session() as session:
                session.write_transaction(create_recebedor_ex1, partido)
            # Crie um relacionamento entre doador e partido
            with driver.session() as session:
                session.write_transaction(create_relacionamento_ex1, doador, partido, valor)


    # Primeiro apresenta os relacionamentos desse exercicio1
    with driver.session() as session:
        session.read_transaction(select_and_display_data_ex1)

    # Depois exclui todos os nodos e relacionamentos desse exercicio1
    #with driver.session() as session:
        #session.write_transaction(delete_all_nodes_and_relationships)    
        
    driver.close()


# Faz o select e mostra os dados - EX1
def select_and_display_data_ex1(tx):
    query = (
        "MATCH (d:Doador)-[r:DOOU]->(p:Recebedor) "
        "RETURN d.nome AS Doador, p.nome AS Recebedor, r.valor AS Valor"
    )
    result = tx.run(query)
    for record in result:

        print(f"Doador: {record['Doador']}\t Recebedor: {record['Recebedor']}\t Valor: {record['Valor']}")

# Função para criar um nodo de doador
def create_doador_ex1(tx, doador, cnpj):
    query = (
        f"MERGE (d:Doador {{nome: '{doador}', cnpj: '{cnpj}'}})"
    )
    tx.run(query)

# Função para criar um nodo de recebedor (partido)
def create_recebedor_ex1(tx, partido):
    query = (
        f"MERGE (r:Recebedor {{nome: '{partido}'}})"
    )
    tx.run(query)

# Função para criar um relacionamento entre doador e recebedor (partido)
def create_relacionamento_ex1(tx, doador, partido, valor):
    query = (
        f"MATCH (d:Doador {{nome: '{doador}'}})"
        f"MATCH (r:Recebedor {{nome: '{partido}'}})"
        f"MERGE (d)-[:DOOU {{valor: {float(valor)}}}]->(r)"
    )
    tx.run(query)



#################EXERCICIO 2 - Doador doou para Governador

# Import dados do CSV para o Neo4j
def exercicio2(uri, username, password, csv_file):
    driver = GraphDatabase.driver(uri, auth=(username, password))

    print("Inserindo dados do CSV - Exercicio 02") 

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        #next(csv_reader)  # Pule o cabeçalho

        # Criação de nós de Doador e Recebedor
        with driver.session() as session:
            for row in csv_reader:
                #print(row)
                nome_doador, cpf_doador, _, _, data, protocolo, valor, tipo, _, governador, governador_number, partido_check, partido, cidade, _, _ = row

                #Os dados do CSV estão errados. Então tive que fazer varios ajustes (Desculpe pelas gambiarras! rsrsrs).
                if governador[0].isdigit():



                    session.write_transaction(create_doador_ex2, nome_doador, cpf_doador)

                    if "Governador" in partido:
                        partido = cidade
                    
                    session.write_transaction(create_recebedor_governador_ex2, governador_number, partido)
                    session.write_transaction(create_relacionamento_ex2, nome_doador, governador_number, valor)

                else:
                    session.write_transaction(create_doador_ex2, nome_doador, cpf_doador)
                    if "Governador" in partido:
                        partido = cidade
                    session.write_transaction(create_recebedor_governador_ex2, governador, partido)
                    session.write_transaction(create_relacionamento_ex2, nome_doador, governador, valor)


                

    # Primeiro apresenta os relacionamentos desse exercicio2
    with driver.session() as session:
        session.read_transaction(select_and_display_data_ex2)

    # Depois exclui todos os nodos e relacionamentos desse exercicio2
    #with driver.session() as session:
    #   session.write_transaction(delete_all_nodes_and_relationships)    
        
    driver.close()




# Função para criar um nó de Doador
def create_doador_ex2(tx, nome, cpf):
    query = (
        f"MERGE (d:Doador {{nome: '{nome}', cpf: '{cpf}'}})"
    )
    tx.run(query)

# Função para criar um nó de Recebedor (Governador)
def create_recebedor_governador_ex2(tx, nome, partido):
    query = (
        f"MERGE (g:Governador {{nome: '{nome}', partido: '{partido}'}})"
    )
    tx.run(query)

#Função para criar os relacionamentos (Doador para Governador)
def create_relacionamento_ex2(tx, doador_nome, governador, valor):
    query = (
        f"MATCH (d:Doador {{nome: '{doador_nome}'}})"
        f"MATCH (g:Governador {{nome: '{governador}'}})"
        f"MERGE (d)-[r:DOOU {{valor: '{valor}'}}]->(g)"
    )
    tx.run(query)



# Faz o select e mostra os dados
def select_and_display_data_ex2(tx):
    query = (
        "MATCH (d:Doador)-[r:DOOU]->(g:Governador) "
        "RETURN d.nome AS Doador, g.nome AS Governador, r.valor AS Valor"
    )
    result = tx.run(query)

    column_width = 30
    
    # Imprime o cabeçalho
    print(f"Doador".ljust(column_width) + f"  Governador".ljust(column_width) + f"  Valor".ljust(column_width))
    
    for record in result:
        doador = record['Doador'][:column_width].ljust(column_width)
        governador = record['Governador'][:column_width].ljust(column_width)
        valor = str(record['Valor'])[:column_width].ljust(column_width)
        print(f"{doador}  {governador}  {valor}")


#################EXERCICIO 3 - Doador doou para Governador

# Import dados do CSV para o Neo4j
def exercicio3(uri, username, password, csv_file):
    driver = GraphDatabase.driver(uri, auth=(username, password))

    print("Inserindo dados do CSV - Exercicio 03") 

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        #next(csv_reader)  # Pule o cabeçalho

        # Criação de nós de Doador e Recebedor
        with driver.session() as session:
            for row in csv_reader:
                #print(row)
                nome_doador, cpf_doador, _, _, data, protocolo, valor, tipo, _, governador, governador_number, partido_check, partido, cidade, _ = row

                #Os dados do CSV estão errados. Então tive que fazer varios ajustes (Desculpe pelas gambiarras! rsrsrs).
                if governador[0].isdigit():



                    session.write_transaction(create_doador_ex3, nome_doador, cpf_doador)

                    if "Governador" in partido:
                        partido = cidade
                    
                    session.write_transaction(create_recebedor_governador_ex3, governador_number, partido)
                    session.write_transaction(create_relacionamento_ex3, nome_doador, governador_number, valor)

                else:
                    session.write_transaction(create_doador_ex3, nome_doador, cpf_doador)
                    if "Governador" in partido:
                        partido = cidade
                    session.write_transaction(create_recebedor_governador_ex3, governador, partido)
                    session.write_transaction(create_relacionamento_ex3, nome_doador, governador, valor)


                

    # Primeiro apresenta os relacionamentos desse exercicio2
    with driver.session() as session:
        session.read_transaction(select_and_display_data_ex3)

    # Depois exclui todos os nodos e relacionamentos desse exercicio2
    #with driver.session() as session:
    #   session.write_transaction(delete_all_nodes_and_relationships)    
        
    driver.close()




# Função para criar um nó de Doador
def create_doador_ex3(tx, nome, cpf):
    query = (
        f"MERGE (d:Doador {{nome: '{nome}', cpf: '{cpf}'}})"
    )
    tx.run(query)

# Função para criar um nó de Recebedor (Governador)
def create_recebedor_governador_ex3(tx, nome, partido):
    query = (
        f"MERGE (g:Governador {{nome: '{nome}', partido: '{partido}'}})"
    )
    tx.run(query)

#Função para criar os relacionamentos (Doador para Governador)
def create_relacionamento_ex3(tx, doador_nome, governador, valor):
    query = (
        f"MATCH (d:Doador {{nome: '{doador_nome}'}})"
        f"MATCH (g:Governador {{nome: '{governador}'}})"
        f"MERGE (d)-[r:DOOU {{valor: '{valor}'}}]->(g)"
    )
    tx.run(query)



# Faz o select e mostra os dados
def select_and_display_data_ex3(tx):
    query = (
        "MATCH (d:Doador)-[r:DOOU]->(g:Governador) "
        "RETURN d.nome AS Doador, g.nome AS Governador, r.valor AS Valor"
    )
    result = tx.run(query)

    column_width = 30
    
    # Imprime o cabeçalho
    print(f"Doador".ljust(column_width) + f"  Governador".ljust(column_width) + f"  Valor".ljust(column_width))
    
    for record in result:
        doador = record['Doador'][:column_width].ljust(column_width)
        governador = record['Governador'][:column_width].ljust(column_width)
        valor = str(record['Valor'])[:column_width].ljust(column_width)
        print(f"{doador}  {governador}  {valor}")



#####QUERY GERAL 
# Deleta todos os nodos e relacionamentos
def delete_all_nodes_and_relationships(tx):
    query = (
        "MATCH (n) DETACH DELETE n"
    )
    tx.run(query)
    print("Dados excluídos!")







#EXERCICIO 01

# CSV 01
csv_file = "csv-data/exercicio01.csv"
exercicio1(uri, username, password, csv_file)

# CSV 02
csv_file = "csv-data/exercicio02.csv"
exercicio2(uri, username, password, csv_file)


# CSV 03
csv_file = "csv-data/exercicio03.csv"
exercicio3(uri, username, password, csv_file)







'''
#EXERCICIO 02

# CSV 02
csv_file = "csv-dataseuarquivo.csv"
exercicio2(uri, username, password, csv_file)


#EXERCICIO 03

# CSV 03
csv_file = "csv-dataseuarquivo.csv"
exercicio3(uri, username, password, csv_file)



'''




