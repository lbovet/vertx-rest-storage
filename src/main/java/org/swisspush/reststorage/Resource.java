package org.swisspush.reststorage;

import io.vertx.core.Handler;

public class Resource implements Comparable<Resource>{
    public String name;
    public boolean exists = true;
    public boolean modified = true;
    public boolean invalid = false;
    public boolean rejected = false;
    public boolean error = false;
    public String invalidMessage;
    public String errorMessage;

    public Handler<String> errorHandler;
    
    @Override
    public int compareTo(Resource o) {
        return this.name.compareTo(o.name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Resource other = (Resource) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }            
}
