import tensorflow as tf

if __name__ == '__main__':
    sess = tf.Session()

    a = tf.Variable(tf.constant(4.0), name="a")
    x_val = 5.0
    x_data = tf.placeholder(dtype=tf.float32, name="placeholder")
    multiplication = tf.multiply(a, x_data, "multiplication")

    loss = tf.square(tf.subtract(multiplication, tf.constant(50.0, dtype=tf.float32, name="target_value")),
                     name="loss_function")
    init = tf.global_variables_initializer()
    sess.run(init)

    my_opt = tf.train.GradientDescentOptimizer(0.01)
    train_step = my_opt.minimize(loss)

    print('Optimizing a Multiplication Gate Output to 50.')
    for i in range(10):
        sess.run(train_step, feed_dict={x_data: x_val})
        a_val = sess.run(a)
        mult_output = sess.run(multiplication, feed_dict={x_data: x_val})
        print(str(a_val) + ' * ' + str(x_val) + ' = ' + str(mult_output))