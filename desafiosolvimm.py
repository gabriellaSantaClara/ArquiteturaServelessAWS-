import boto3
from boto3.dynamodb.conditions import Key

def upload_files(file_name, bucket):
    object_name=None
    args=None
    if object_name is None:
        object_name = file_name
        
    response = client.upload_file(file_name, bucket, object_name, ExtraArgs = args)
    
    print(response)

def criar():
    ##criar tabela metadado no DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName = 'Metadadado',
        KeySchema = [
            {
             'AttributeName' : 'tamanho',
             'KeyType' : 'HASH'   
            },
            {
              'AttributeName' : 'tipo',
              'KeyType' : 'RANGE'   
            }
        
        ],
  
        AttributeDefinitions = [
       
            {
                'AttributeName' : 'tamanho',
                'AttributeType' : 'N',
            },
            {
                'AttributeName' : 'tipo',
                'AttributeType' : 'S',
            }
        
       
        ], 
        ProvisionedThroughput = {
            'ReadCapacityUnits' : 1,
            'WriteCapacityUnits' : 1
        }
        )
 
def ExtractMetadata():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Metadadado')   
    ## inserir registro na tabela metadado
    table.put_item(
    Item = {
    'tamanho': 1191,
    'tipo': '.PNG',
    'dimensao': '823X550'    
   
    }    
    )
    table.put_item(
    Item = {
    'tamanho': 711,
    'tipo': '.PNG',
    'dimensao': '631X421'   
   
    }    
    )
    table.put_item(
    Item = {
    'tamanho': 972,
    'tipo': '.PNG',
    'dimensao': '826X550'   
   
    }    
    )

def getMetadata():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Metadadado')
    ## resgata o valor da tabela metadado
    resp = table.scan()
    outro = resp['Items']
    print('Dados da tabela:')
    print(outro)

def getImage():
    s3 = boto3.resource('s3')
    s3.buckets.all()
    bucket = s3.Bucket('desafiosolvimm')
    files = list(bucket.objects.all())
    files
    for file in files:
        s3 = boto3.resource('s3')
        s3.buckets.all()
        bucket = s3.Bucket('desafiosolvimm')
        files = list(bucket.objects.all())
        ##realiza o download de cada imagem 
        client.download_file('desafiosolvimm', file.key, file.key)

def InfoImages():  
    ##encontrando o registro de menor tamanho 
    aux = 0
    menor = 99999999
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Metadadado')
    resp = table.scan()
    outro = resp['Items']
    for x in resp['Items']:    
        if(outro[aux]['tamanho'] < menor):
            menor = outro[aux]['tamanho']        
        aux = aux + 1 
    print('O menor tamanho:')
    print(menor)
    response = table.query(
    KeyConditionExpression=Key('tamanho').eq(menor)    
    ) 
    imprime = response['Items']
    print('Dados do registro de menor tamanho:') 
    print(imprime)
    ##encontrando o registro de maior tamanho 
    aux = 0
    maior = 0 
    for x in resp['Items']:
    
        if(outro[aux]['tamanho'] > maior):
            maior = outro[aux]['tamanho']        
        aux = aux + 1 
    print('O maior tamanho:')
    print(maior)
    response = table.query(
    KeyConditionExpression=Key('tamanho').eq(maior)    
    ) 
    imprime = response['Items']
    print('Dados do registro de maior tamanho:') 
    print(imprime)
    ##tipos de imagens
    aux = 0
    print("O tipo de cada imagem:")
    for x in resp['Items']:    
        print(outro[aux]['tipo'])          
        aux = aux + 1 


if __name__ == "__main__":
    #Criar cliente
    client = boto3.client('s3') #criar cliente conectado com o s3  
    client.create_bucket(Bucket = 'desafiosolvimm') #criar o bucket
    response = client.list_buckets() 
    response['Buckets']
    
    ##Upload de arquivo
    #upload_files('imagem1.png', 'desafiosolvimm')
    #upload_files('imagem2.png', 'desafiosolvimm')
    #upload_files('imagem3.png', 'desafiosolvimm')
    
    #Criar tabela 
    #criar()
    #dynamodb = boto3.resource('dynamodb')
    #table = dynamodb.Table('Metadadado_29')
   
    ##Armazenar metadados no Dynamodb
    ExtractMetadata()
    
    ##retornar os metadados armazenados no DynamoDB.
    getMetadata()
    
    ##download da imagem
    getImage()
    
    ##pesquisa os metadados salvos no DynamoDB
    InfoImages()
 

    
   
                   
