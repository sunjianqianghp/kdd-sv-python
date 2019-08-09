import tensorflow as tf

if __name__ == '__main__':
    sess = tf.Session()

    a = tf.constant([1.0, 2.0], name="a")
    b = tf.constant([2.0, 3.0], name="b")
    result = tf.add(a, b, name="add")
    print(result)

    print(tf.get_default_graph())
    writer = tf.summary.FileWriter("C:/sunjianqiang/tensorboard/log", tf.get_default_graph())
    writer.close()