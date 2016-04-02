/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package org.swisspush.reststorage;

import redis.clients.jedis.Jedis;

/**
 * Created by kammermannf on 07.06.2015.
 */
public class JedisFactory {

    public static Jedis createJedis() {
        return new Jedis("localhost", 6379, 5000);
    }
}
