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
package com.facebook.presto.mysql;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLTypeWithTypeArguments
        implements FullMySQLType
{
    private final MYSQLType mySQLType;
    private final List<MYSQLType> typeArguments;

    public MySQLTypeWithTypeArguments(MYSQLType mySQLType, List<MYSQLType> typeArguments)
    {
        this.mySQLType = checkNotNull(mySQLType, "mySQLType is null");
        this.typeArguments = checkNotNull(typeArguments, "typeArguments is null");
    }

    public MYSQLType getMySQLType()
    {
        return mySQLType;
    }

    public List<MYSQLType> getTypeArguments()
    {
        return typeArguments;
    }

    @Override
    public String toString()
    {
        if (typeArguments != null) {
            return mySQLType.toString() + typeArguments.toString();
        }
        else {
            return mySQLType.toString();
        }
    }
}
