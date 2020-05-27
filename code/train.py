import tensorflow as tf
import numpy as np
import os
import time
import csv
import re
import datetime
import data_helpers
from text_cnn import TextCNN
from tensorflow.contrib import learn
import pandas as pd
from gensim.models import KeyedVectors
from keras.preprocessing.sequence import pad_sequences
from keras.utils import to_categorical
from ast import literal_eval


# Parameters
# ==================================================

# Data loading params
tf.flags.DEFINE_float("dev_sample_percentage", .1, "Percentage of the training data to use for validation")

# Model Hyperparameters
##tf.flags.DEFINE_integer("embedding_dim", 128, "Dimensionality of character embedding (default: 128)")
tf.flags.DEFINE_string("filter_sizes", "3,4,5", "Comma-separated filter sizes (default: '3,4,5')")
tf.flags.DEFINE_integer("num_filters", 128, "Number of filters per filter size (default: 128)")
tf.flags.DEFINE_float("dropout_keep_prob", 0.5, "Dropout keep probability (default: 0.5)")
tf.flags.DEFINE_float("l2_reg_lambda", 0.0, "L2 regularization lambda (default: 0.0)")

# Training parameters
tf.flags.DEFINE_integer("batch_size", 64, "Batch Size (default: 64)")
tf.flags.DEFINE_integer("num_epochs", 200, "Number of training epochs (default: 200)")
tf.flags.DEFINE_integer("evaluate_every", 10, "Evaluate model on dev set after this many steps (default: 100)")
tf.flags.DEFINE_integer("checkpoint_every", 100, "Save model after this many steps (default: 100)")
tf.flags.DEFINE_integer("num_checkpoints", 5, "Number of checkpoints to store (default: 5)")
# Misc Parameters
tf.flags.DEFINE_boolean("allow_soft_placement", True, "Allow device soft device placement")
tf.flags.DEFINE_boolean("log_device_placement", False, "Log placement of ops on devices")

FLAGS = tf.flags.FLAGS


"""
create training and testing samples
and load pre-trained embedding matrix
"""
def createData(mode="relev"):
    print("Loading data...")
    if mode=="primarySIC":
        ##y
        naics = pd.read_csv("../../dataMichael/Orbis/orbis_industries/US_2_Empl/US_2_Empl_BvDID_NAICS.csv",sep=";")
        ##fill missing bvdids
        naics["bvdidnumber"] = naics["bvdidnumber"].fillna(method='ffill')
        ##drop NaN obs resp.
        primary = naics[naics["naics2017primarycodes"].notna()]
        primary = primary[["bvdidnumber","naics2017primarycodes"]]
        ##X
        ##DATA_EMBEDDED.CSV MOST CURRENT FILE?
        data = pd.read_csv("data_embedded.csv", sep=";")
        data = data[data["text2id"]!="[]"]
        ##agg word ids by bvdid
        data["text2id"] = data["text2id"].apply(literal_eval)
        Xtemp = data.groupby(["bvdid"]).agg({"text2id": "sum"})
        X = pd.DataFrame(Xtemp["text2id"].to_list())
        ##such that NA is not considered for one_hot
        X = X.fillna(embedEN.shape[0])
        X = X.apply(lambda x: x.apply(int,1),0)
        X["bvdid"] = Xtemp.index
        ##merge labels with input data
        final = pd.merge(X,primary,"inner",left_on="bvdid",right_on="bvdidnumber"); del(X)
        size = int(final.shape[0]*(1-FLAGS.dev_sample_percentage))
    elif mode=="relev":
        data = pd.read_csv("../dataBetweenRelevance.csv", sep=";", encoding="utf-8", quoting=csv.QUOTE_ALL)
        data = data.drop(["bvdidID", "urlID_x"], axis=1)
        data["text2id"] = data["text2id"].apply(lambda x: re.findall("[0-9]+",x))
        data["lens"] = [len(i) for i in data["text2id"]]
        data = data[data["lens"]!=0]
        X = data["text2id"]
        ##cast y to categorical via one-hot encoding
        y = pd.DataFrame(to_categorical(data["relevant"]))
        size = int(data.shape[0]*(1-FLAGS.dev_sample_percentage))
    ##random selection of data for train and test data (neglecting order)
    ##to-do: implement cross-validation
    np.random.seed(123)
    ##to-do: stratified train and dev set construction! (~y)
    train = np.random.choice(data.shape[0], size, replace=False)
    dev = list(set(range(data.shape[0]))-set(train))
    ##padding to 0 (,)
    X = pad_sequences(X)
    ##make input space smaller
    X = X[:,0:5000]
    train_X = X[train,:]
    train_y = y.iloc[train]
    ##make dev space smaller, due to OOM 
    dev_X = X[dev[0:100],:]#X[dev,:]
    dev_y = y.iloc[dev[0:100]]#y.iloc[dev]
    embedEN = KeyedVectors.load_word2vec_format("../pretrainedModels/crawl-300d-2M.vec")
    return train_X, train_y, dev_X, dev_y, embedEN


"""
Generates a batch iterator for a dataset.
"""
def batch_iter(X, y, batch_size, num_epochs, shuffle=True):
    y = np.array(y)
    data_size = X.shape[0]
    num_batches_per_epoch = int((data_size-1)/batch_size) + 1
    for epoch in range(num_epochs):
        # Shuffle the data at each epoch
        if shuffle:
            shuffle_indices = np.random.permutation(np.arange(data_size))
            shuffled_X = X[shuffle_indices]
            shuffled_y = y[shuffle_indices]
        else:
            shuffled_X = X
            shuffled_y = y
        for batch_num in range(num_batches_per_epoch):
            start_index = batch_num * batch_size
            end_index = min((batch_num + 1) * batch_size, data_size)
            yield shuffled_X[start_index:end_index], shuffled_y[start_index:end_index]

            
def train(x_train, y_train, x_dev, y_dev, embedEN):
    embedEN = embedEN.wv.syn0 #np.random.rand(2000000,300)
    # Training
    # ==================================================
    with tf.Graph().as_default():
        session_conf = tf.ConfigProto(
          allow_soft_placement=FLAGS.allow_soft_placement,
          log_device_placement=FLAGS.log_device_placement)
        sess = tf.Session(config=session_conf)
        with sess.as_default():
            cnn = TextCNN(
                sequence_length=x_train.shape[1],
                num_classes=y_train.shape[1],
                vocab_size=embedEN.shape[0],
                embedding_size=embedEN.shape[1],
                filter_sizes=list(map(int, FLAGS.filter_sizes.split(","))),
                num_filters=FLAGS.num_filters,
                l2_reg_lambda=FLAGS.l2_reg_lambda)

            # Define Training procedure
            global_step = tf.Variable(0, name="global_step", trainable=False)
            optimizer = tf.train.AdamOptimizer(1e-4)
            grads_and_vars = optimizer.compute_gradients(cnn.loss)
            train_op = optimizer.apply_gradients(grads_and_vars, global_step=global_step)

            # Keep track of gradient values and sparsity (optional)
            # grad_summaries = []
            # for g, v in grads_and_vars:
            #     if g is not None:
            #         grad_hist_summary = tf.summary.histogram("{}/grad/hist".format(v.name), g)
            #         sparsity_summary = tf.summary.scalar("{}/grad/sparsity".format(v.name), tf.nn.zero_fraction(g))
            #         grad_summaries.append(grad_hist_summary)
            #         grad_summaries.append(sparsity_summary)
            # grad_summaries_merged = tf.summary.merge(grad_summaries)

            # Output directory for models and summaries
            timestamp = str(int(time.time()))
            out_dir = os.path.abspath(os.path.join(os.path.curdir, "runs", timestamp))
            print("Writing to {}\n".format(out_dir))

            # Summaries for loss and accuracy
            loss_summary = tf.summary.scalar("loss", cnn.loss)
            acc_summary = tf.summary.scalar("accuracy", cnn.accuracy)

            # Train Summaries
            train_summary_op = tf.summary.merge([loss_summary, acc_summary]) #, grad_summaries_merged
            train_summary_dir = os.path.join(out_dir, "summaries", "train")
            train_summary_writer = tf.summary.FileWriter(train_summary_dir, sess.graph)

            # Dev summaries
            dev_summary_op = tf.summary.merge([loss_summary, acc_summary])
            dev_summary_dir = os.path.join(out_dir, "summaries", "dev")
            dev_summary_writer = tf.summary.FileWriter(dev_summary_dir, sess.graph)

            # Checkpoint directory. Tensorflow assumes this directory already exists so we need to create it
            checkpoint_dir = os.path.abspath(os.path.join(out_dir, "checkpoints"))
            checkpoint_prefix = os.path.join(checkpoint_dir, "model")
            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir)
            saver = tf.train.Saver(tf.global_variables(), max_to_keep=FLAGS.num_checkpoints)

            # Write vocabulary
            # vocab_processor.save(os.path.join(out_dir, "vocab"))

            # Initialize all variables
            sess.run(tf.global_variables_initializer())

            #Initialize word embedding tensor
            initEmbed = sess.run(cnn.activateW, feed_dict={cnn.place: embedEN})

            # print(cnn.W.eval())

            def train_step(x_batch, y_batch):
                """
                A single training step
                """
                feed_dict = {
                  cnn.input_x: x_batch,
                  cnn.input_y: y_batch,
                  cnn.dropout_keep_prob: FLAGS.dropout_keep_prob
                }
                _, step, summaries, loss, accuracy = sess.run(
                    [train_op, global_step, train_summary_op, cnn.loss, cnn.accuracy], feed_dict)
                time_str = datetime.datetime.now().isoformat()
                print("{}: step {}, loss {:g}, acc {:g}".format(time_str, step, loss, accuracy))
                train_summary_writer.add_summary(summaries, step)

            def dev_step(x_batch, y_batch, writer=None):
                """
                Evaluates model on a dev set
                """
                feed_dict = {
                  cnn.input_x: x_batch,
                  cnn.input_y: y_batch,
                  cnn.dropout_keep_prob: 1.0
                }
                step, summaries, loss, accuracy = sess.run(
                    [global_step, dev_summary_op, cnn.loss, cnn.accuracy],
                    feed_dict)
                time_str = datetime.datetime.now().isoformat()
                print("{}: step {}, loss {:g}, acc {:g}".format(time_str, step, loss, accuracy))
                if writer:
                    writer.add_summary(summaries, step)

            # Generate batches
            batches = batch_iter(x_train, y_train, FLAGS.batch_size, FLAGS.num_epochs)
            # Training loop. For each batch...
            for batch in batches:
                x_batch, y_batch = batch
                train_step(x_batch, y_batch)
                current_step = tf.train.global_step(sess, global_step)
                if current_step % FLAGS.evaluate_every == 0:
                    print("\nEvaluation:")
                    dev_step(x_dev, y_dev, writer=dev_summary_writer)
                    print("")
                if current_step % FLAGS.checkpoint_every == 0:
                    path = saver.save(sess, checkpoint_prefix, global_step=current_step)
                    print("Saved model checkpoint to {}\n".format(path))


def main(argv=None):
    x_train, y_train, x_dev, y_dev, embedEN = createData(mode="relev")
    train(x_train, y_train, x_dev, y_dev, embedEN)

    
if __name__ == '__main__':
    tf.app.run()
