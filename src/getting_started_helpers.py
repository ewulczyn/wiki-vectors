import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE


def get_tsne(embedding, pca_dim = 20, n_words=10000):
    """
    TSNE dimensionality reduction.
    
    The TSNE algorithm is quite slow, so we:
    
    1. only use the first n_words from the embedding
    2. reduce embedding dimensionality via PCA
    3. run TSNE on reduced embedding matrix
    
    """
    X = embedding.E[:n_words]
    pca = PCA(n_components=pca_dim)
    X = pca.fit_transform(X)
    tsne = TSNE(n_components=2, random_state=0)
    return tsne.fit_transform(X)



def repel_labels(ax, x, y, labels, k=10):
    """
    Helper code for making a readable scatter plot: See:
    https://stackoverflow.com/questions/14938541/how-to-improve-the-label-placement-for-matplotlib-scatter-chart-code-algorithm
    """
    G = nx.DiGraph()
    data_nodes = []
    init_pos = {}
    for xi, yi, label in zip(x, y, labels):
        data_str = 'data_{0}'.format(label)
        G.add_node(data_str)
        G.add_node(label)
        G.add_edge(label, data_str)
        data_nodes.append(data_str)
        init_pos[data_str] = (xi, yi)
        init_pos[label] = (xi, yi)

    pos = nx.spring_layout(G, pos=init_pos, fixed=data_nodes, k=k)

    # undo spring_layout's rescaling
    pos_after = np.vstack([pos[d] for d in data_nodes])
    pos_before = np.vstack([init_pos[d] for d in data_nodes])
    scale, shift_x = np.polyfit(pos_after[:,0], pos_before[:,0], 1)
    scale, shift_y = np.polyfit(pos_after[:,1], pos_before[:,1], 1)
    shift = np.array([shift_x, shift_y])
    for key, val in pos.items():
        pos[key] = (val*scale) + shift

    for label, data_str in G.edges():
        ax.annotate(label,
                    xy=pos[data_str], xycoords='data',
                    xytext=pos[label], textcoords='data',
                    arrowprops=dict(arrowstyle="->",
                                    shrinkA=0, shrinkB=0,
                                    connectionstyle="arc3", 
                                    color='red'), )
    # expand limits
    all_pos = np.vstack(pos.values())
    x_span, y_span = np.ptp(all_pos, axis=0)
    mins = np.min(all_pos-x_span*0.15, 0)
    maxs = np.max(all_pos+y_span*0.15, 0)
    ax.set_xlim([mins[0], maxs[0]])
    ax.set_ylim([mins[1], maxs[1]])

def plot_tsne(embedding, tsne, n=20):
    """
    Create a scatter plot for the first
    n words in embedding using tsne representation
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.scatter(tsne[:n, 0], tsne[:n, 1])
    repel_labels(ax, tsne[:n, 0], tsne[:n, 1], embedding.idx2w[:n], k=0.04)
    plt.show()
    fig.savefig('embedding.png')


def plot_tsne_simple(embedding, tsne):
    plt.figure(figsize=(8, 8))
    for i in range(20):
        x, y = tsne_embedding[i,:]
        plt.scatter(x, y)
        plt.annotate(embedding.idx2w[i],
                     xy=(x, y),
                     xytext=(5, 2),
                     textcoords='offset points',
                     ha='right',
                     va='bottom')