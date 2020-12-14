import numpy as np

def main():
    Demanda=[[1.95, 0.90, 21],[0.18, 0.18, 0.0]]
    Z=[180,3]
    x0=0.0
    count = 0 
    
    
    inicia = np.zeros((100, 100, 3))  # total de clintes de todas as classes no recurso i     
    
    for a in range(11):
        total = 0
        for b in range(41): 
            array=[]
            resto = 1
            for count in range(1):              
                array.append(a)
                array.append(b)
            
            nb=[1000]
            ni=[1000]
            
            #tupla de população de cada classe em cada interação 
            for r in range(2): # cada classe de cliente                  
                tempo_residencia=[]
                tempo_total = 0
                # quando os recursos estão vazios o tempo de residencia e zero
                if (a == 0) and (b == 0):
                    for i in range(3):
                        rt = 0
                        tempo_residencia.append(rt)
                        p = 0
                        array.append(p)
                #depois que os recursos começam a
                if (a != 0) or (b != 0):
                    for i in range(3): #interação em cada recurso
                        if a == 0: #primeira interação para o batch
                                   
                                if r == 0: #batch
                                    tempo_residencia_soma = float(round(Demanda[r][i]*(1+inicia [a][b][i]),2))
                                    tempo_residencia.append(tempo_residencia_soma) #tempo de residencias
                                    array.append(tempo_residencia_soma)
                                    tempo_total = tempo_total + tempo_residencia_soma
                                else:  #interativo
                                    tempo_residencia_soma = float(round(Demanda[r][i]*(1+inicia [a][b-1][i]),2))
                                    tempo_residencia.append(tempo_residencia_soma) #tempo de residencias
                                    array.append(tempo_residencia_soma)
                                    tempo_total = tempo_total + tempo_residencia_soma
                        else:
                                    
                                if r == 0:  #batch
                                    tempo_residencia_soma = float(round(Demanda[r][i]*(1+inicia [a-1][b][i]),2))                                       
                                    tempo_residencia.append(tempo_residencia_soma)
                                    array.append(tempo_residencia_soma)
                                    tempo_total = tempo_total + tempo_residencia_soma
                                else: #interativo
                                    tempo_residencia_soma = float(round(Demanda[r][i]*(1+inicia [a][b-1][i]),2))
                                    tempo_residencia.append(tempo_residencia_soma) #tempo de residencias
                                    array.append(tempo_residencia_soma)
                                    tempo_total = tempo_total + tempo_residencia_soma
                   
                
                if (resto%2) == 1:
                     x0=float((a/(Z[r]+tempo_total))) 
                     x0 = (round(x0,4))
                     array.append(x0)
                     resto = resto + 1
                else:
                     x0=float((total/(Z[r]+tempo_total))) 
                     x0 = (round(x0,4))
                     array.append(x0)
                     total = total + 1
                     resto = resto + 1

                #Calculo ni     
                if r == 0:
                    nb = [x0*i for i in tempo_residencia] 
                    for i in nb:
                        parametro = float(round(i,3))
                        array.append(parametro)
                else:
                    ni = [x0*i for i in tempo_residencia]
                    for i in ni:
                        parametro = float(round(i,3))
                        array.append(parametro)                                                  
                                                       
                                                    
            inicia [a][b]=[i+o for i,o in zip(nb, ni)] #atualização do inicia 

            #imprimir        
            string = len(array)
            print('\n')
            for count in range(string):
                print('|' + str(array[count]))
           
    print('n1|n2|Rb_cpu|Rb_dis|Rb_h|x0b|nb_cpu|nb_dis|nb_hd|Ri_cpu|Ri_dis|Ri_hd|x0i|ni_cpu|ni_dis|ni_hd')
       

if __name__ == "__main__":
    main()