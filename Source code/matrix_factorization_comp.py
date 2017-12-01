#=======================================================
#Import packages
#=======================================================
from matrix_factorization import get_user_mvrating_DF
from numpy import *
import operator
from os import listdir
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from numpy.linalg import *
from scipy.sparse.linalg import svds
from scipy.stats.stats import pearsonr
from numpy import linalg as la
matplotlib.style.use('ggplot')


#============================================================================
#Get input data matrix
#============================================================================
#usr_mvrating_matrix = get_user_mvrating_DF()
# usr_genre_matrix = usr_genre_matrix.T
# pprint.pprint(usr_genre_matrix)
#usr_mvrating_matrix.to_csv("factorization_1_user_mvrating.csv", sep='\t')

# load data points
#with open("factorization_1_user_mvrating.csv") as f:
with open("factorization_1_user_mvrating_svd_usr_grt_70k.csv") as f:
    ncols = len(f.readline().split('\t'))

#R = pd.DataFrame(loadtxt('factorization_1_user_mvrating.csv',delimiter='\t', skiprows=1, usecols=range(1,ncols)))
R = loadtxt('factorization_1_user_mvrating_svd_usr_grt_70k.csv',delimiter='\t', skiprows=1, usecols=range(1,ncols))


#kvm known_value_matrix = for values in R[i,j] >0, kvm[i,j] = 1
kvm = R>0.5
kvm[kvm == True] = 1
kvm[kvm == False] = 0
# To be consistent with our R matrix
kvm = kvm.astype(float64, copy=False)

#print kvm
#latent_sem = 86

#=================================================================================
#Calculate the SVD
#The k value is 86 as selected by the packge by default,as number of movies is 86
#=================================================================================

#How can I select a reduced number of latent semantics here?
#latent_sem = 86
#U, s, V = linalg.svd(R, full_matrices=False)
k_topics = 50
U, s, V = svds(R, k=k_topics)
#s = linalg.svd(raw_data, full_matrices=False, compute_uv = False)

#U_df = pd.DataFrame(U)
#s_df = pd.DataFrame(s)
#V_df = pd.DataFrame(V)


#U_df.to_csv("svd_U.csv",sep='\t')
#s_df.to_csv("svd_s.csv",sep='\t')
#V_df.to_csv("svd_V.csv",sep='\t')


#===============================================================================================
#Read data from CSV gives error while reconstruction and convert to dataframe. So no need to compute the SVD for each program run.
#This way we get the reconstruction matrix.
#===============================================================================================

#U_df = pd.DataFrame(loadtxt('svd_U.csv',delimiter='\t',skiprows=1)).T
#s_df = pd.DataFrame(loadtxt('svd_s.csv',delimiter='\t',skiprows=1))
#V_df = pd.DataFrame(loadtxt('svd_V.csv',delimiter='\t',skiprows=1)).T

#print U.shape, s.shape, V.shape

#Generate the Sig matrix of size latent_sem X latent_sem with the values in the diagonal
Sig= zeros((k_topics, k_topics), dtype=complex)
Sig = diag(s)
pd.DataFrame(Sig).to_csv("Sig_df.csv",sep='\t')

#Q = pd.DataFrame(U)
Q=U

#Sig_df = pd.DataFrame(Sig)
Sig_df = Sig

#V_df = pd.DataFrame(V)
V_df = V

#print shape(Sig_df),shape(V_df)
#P = pd.DataFrame.dot(Sig_df,V_df)
P = dot(Sig_df,V_df)

#lb = regularization constant.
ld=0.1
#reg_value = ld*(sum(Q**2) + sum(P**2))

weighted_errors = []
n_iterations = 50 #After checking for 10,20,100 iterations, found that errors start to converge after 50 interations

#print Q.shape
#print P.shape
#R_df = pd.DataFrame(R)

#print "------------------"

for ii in range(n_iterations):
    for u, Wu in enumerate(kvm):
        #print Wu
        #print diag(Wu)
        Q[u] = linalg.solve(dot(P, dot(diag(Wu), P.T)) + ld * eye(k_topics),dot(P, dot(diag(Wu), R[u].T))).T
        #Q[u] = linalg.solve(dot(P,P.T) + ld * eye(k_topics), dot(P,R[u].T)).T

    for i, Wi in enumerate(kvm.T):
        P[:, i] = linalg.solve(dot(Q.T, dot(diag(Wi), Q)) + ld * eye(k_topics), dot(Q.T, dot(diag(Wi), R[:, i])))
        #P[:, i] = linalg.solve(dot(Q.T, Q) + ld * eye(k_topics), dot(Q.T, R[:, i]))


    R_prime = dot(Q, P)

    MSE = sum((kvm * (R - R_prime)) ** 2)
    print "Error = ", MSE
    weighted_errors.append(MSE)
    #weighted_errors.append(sum((kvm * (R - R_prime))**2))
    #print weighted_errors
    print('{}th iteration is completed'.format(ii))

weighted_R_hat = dot(Q, P)
weighted_R_df = pd.DataFrame(weighted_R_hat);
weighted_R_df.to_csv("R_final_svd_usr_grt_70k.csv",sep='\t')
