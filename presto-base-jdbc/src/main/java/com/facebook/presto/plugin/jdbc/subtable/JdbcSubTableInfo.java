/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc.subtable;

public class JdbcSubTableInfo
{
    private String catalogname;
    private String schemaname;
    private String tablename;
    private String id;
    private String basecatalog;
    private String baseschema;
    private String basetable;
    private String connectionURL;
    private String host;
    private String remotelyaccessible;
    private int scannodenumber;
    private String autoincrementfield;

    private String uid;
    private String username;
    private String password;
    private long fieldMaxValue;
    private long fieldMinValue;
    private String pdboEnable;

    public static final String COLUMN_NAME = "connectionurl,schemaname,tablename,basecatalog,baseschema,basetable,host,"
            + "remotelyaccessible,scannodenumber,autoincrementfield,username,password,fieldmaxvalue,fieldminvalue,pdboenable";

    public String getCatalogname()
    {
        return catalogname;
    }

    public void setCatalogname(String catalog)
    {
        this.catalogname = catalog;
    }

    public String getSchemaname()
    {
        return schemaname;
    }

    public void setSchemaname(String schema)
    {
        this.schemaname = schema;
    }

    public String getTablename()
    {
        return tablename;
    }

    public void setTablename(String table)
    {
        this.tablename = table;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getBasecatalog()
    {
        return basecatalog;
    }

    public void setBasecatalog(String basecatalog)
    {
        this.basecatalog = basecatalog;
    }

    public String getBaseschema()
    {
        return baseschema;
    }

    public void setBaseschema(String baseschema)
    {
        this.baseschema = baseschema;
    }

    public String getBasetable()
    {
        return basetable;
    }

    public void setBasetable(String basetable)
    {
        this.basetable = basetable;
    }

    public String getConnectionURL()
    {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL)
    {
        this.connectionURL = connectionURL;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public boolean getRemotelyaccessible()
    {
        if (remotelyaccessible == null || "".equals(remotelyaccessible)) {
            return true;
        }
        else if ("Y".equals(remotelyaccessible)
                || "y".equals(remotelyaccessible)) {
            return true;
        }
        else {
            return false;
        }
    }

    public void setRemotelyaccessible(String remotelyaccessible)
    {
        this.remotelyaccessible = remotelyaccessible;
    }

    public int getScannodenumber()
    {
        return scannodenumber;
    }

    public void setScannodenumber(int scannodenumber)
    {
        this.scannodenumber = scannodenumber;
    }

    public String getAutoincrementfield()
    {
        return autoincrementfield;
    }

    public void setAutoincrementfield(String autoincrementfield)
    {
        this.autoincrementfield = autoincrementfield;
    }

    public String getUid()
    {
        return uid;
    }

    public void setUid(String uid)
    {
        this.uid = uid;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public long getFieldMaxValue()
    {
        return fieldMaxValue;
    }

    public void setFieldMaxValue(long fieldMaxValue)
    {
        this.fieldMaxValue = fieldMaxValue;
    }

    public long getFieldMinValue()
    {
        return fieldMinValue;
    }

    public void setFieldMinValue(long fieldMinValue)
    {
        this.fieldMinValue = fieldMinValue;
    }

    public boolean isPdboEnable()
    {
        if (pdboEnable == null || "".equals(pdboEnable)) {
            return false;
        }
        else if ("Y".equals(pdboEnable)
                || "y".equals(pdboEnable)) {
            return true;
        }
        else {
            return false;
        }
    }

    public void setPdboEnable(String pdboEnable)
    {
        this.pdboEnable = pdboEnable;
    }
}
