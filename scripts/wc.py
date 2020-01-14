from wordcloud import WordCloud, STOPWORDS
import matplotlib
import matplotlib.pyplot as plt
matplotlib.rcParams['figure.figsize'] = (16.0, 9.0)

def create_hashtag_wordcloud(input_dict, filename, output):
    hashtags_freq_dict = {k: v[0] for k, v in input_dict.items()}

    # Set of Stop words
    additional_stopwords = []
    
    stopwords = additional_stopwords + list(STOPWORDS)

    # Create WordCloud Object
    wc = WordCloud(collocations=False, background_color="white", stopwords=stopwords, width=1600, height=900, colormap=matplotlib.cm.inferno)

    # Generate WordCloud
    wc.generate_from_frequencies(hashtags_freq_dict)

    # Saves the WordCloud
    plt.figure()
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(output+'/'+filename)