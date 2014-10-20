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
package com.facebook.presto.cli;

import com.facebook.presto.cli.Tuple.Pair;
import com.facebook.presto.cli.Tuple.Triple;
import com.facebook.presto.client.StatementClient;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;

import io.airlift.units.Duration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OutputHandler
        implements Closeable
{
    private static final Duration MAX_BUFFER_TIME = new Duration(3, SECONDS);
    private static final int MAX_BUFFERED_ROWS = 10_000;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final OutputPrinter printer;

    private long bufferStart;

    public OutputHandler(OutputPrinter printer)
    {
        this.printer = checkNotNull(printer, "printer is null");
    }

    public void processRow(List<?> row)
            throws IOException
    {
        if (rowBuffer.isEmpty()) {
            bufferStart = System.nanoTime();
        }

        rowBuffer.add(row);
        if (rowBuffer.size() >= MAX_BUFFERED_ROWS) {
            flush(false);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            flush(true);
            printer.finish();
        }
    }

    public void processRows(StatementClient client, HashMap<Integer,Tuple.Triple<String,String,String>> filterMap)
            throws IOException
    {
        while (client.isValid()) {
            Iterable<List<Object>> data = client.current().getData();
            if (data != null) {
                for (List<Object> tuple : data) {
                	
                	for(Entry<Integer, Triple<String, String, String>> filterEntry: filterMap.entrySet()) {
                		Integer itemIndex = filterEntry.getKey();
                		Object item = tuple.get(itemIndex.intValue());
                		System.out.println(item);
                		JSONObject jsonRep = null;
                		try {
                			if(item != null)
                       		 jsonRep = new JSONObject(item.toString());
                			 
                			 switch(filterEntry.getValue().getMiddle()) {
                			 	case Constants.EQ:
                			 		String actualStringValue = (String)jsonRep.get(filterEntry.getValue().getKey());
                			 		if (!filterEntry.getValue().getValue().equals(actualStringValue)) {
                			 			System.out.println("One of the expected & actual values not equal in EQ operator or FILTER_WITH clause"); 
                			 			return;
                			 		}
                			 		break;
                			 	case Constants.LT:
                			 		try {
                    			 		char firstLTChar = filterEntry.getValue().getKey().charAt(0);
                    			 		String ltKey = filterEntry.getValue().getKey().substring(1);
                    			 		String actualLTValue = (String) jsonRep.get(ltKey);
                    			 		
                    			 		if(firstLTChar == 'l') {
                    			 			Long actualLongValue = Long.parseLong(actualLTValue);
                    			 			Long expectedLongValue = Long.parseLong(filterEntry.getValue().getValue());
                    			 			if(! (actualLongValue < expectedLongValue)) {
                    			 				System.out.println("One of the actual values not less than expected value in LT operator or FILTER_WITH clause"); 
                    			 				return;
                    			 			}
                    			 		}
                    			 		else if(firstLTChar == 'd') {
                    			 			Double actualDoubleValue = Double.parseDouble(actualLTValue);
                    			 			Double expectedDoubleValue = Double.parseDouble(filterEntry.getValue().getValue());
                    			 			if(! (actualDoubleValue < expectedDoubleValue)) {
                    			 				System.out.println("One of the actual values not less than expected value in LT operator or FILTER_WITH clause");
                    			 				return;
                    			 			}
                    			 		}
                			 		} catch (Exception e) {
                			 			System.out.println("For <, and > operations use 'l', 'd' to notify if it is a long or double value for comparision in FILTER_WITH clause.");
                			 			return;
                			 		}
                			 		break;
                			 	case Constants.GT:
                			 		try {
                			 			char firstGTChar = filterEntry.getValue().getKey().charAt(0);
                    			 		String gtKey = filterEntry.getValue().getKey().substring(1);
                    			 		String actualGTValue = (String) jsonRep.get(gtKey);
                    			 		
                    			 		if(firstGTChar == 'l') {
                    			 			Long actualLongValue = Long.parseLong(actualGTValue);
                    			 			Long expectedLongValue = Long.parseLong(filterEntry.getValue().getValue());
                    			 			if(! (actualLongValue > expectedLongValue)) {
                    			 				System.out.println("One of the actual values not greater than expected value in GT operator or FILTER_WITH clause");
                    			 				return;
                    			 			}
                    			 		}
                    			 		else if(firstGTChar == 'd') {
                    			 			Double actualDoubleValue = Double.parseDouble(actualGTValue);
                    			 			Double expectedDoubleValue = Double.parseDouble(filterEntry.getValue().getValue());
                    			 			if(! (actualDoubleValue > expectedDoubleValue)) {
                    			 				System.out.println("One of the actual values not greater than expected value in GT operator or FILTER_WITH clause");
                    			 				return;
                    			 			}
                    			 		}
                			 		}catch (Exception e) {
                			 			System.out.println("For <, and > operations use 'l', 'd' to notify if it is a long or double value for comparision in FILTER_WITH clause.");
                			 			return;
                			 		}
                			 		break;
                			 	default:
                			 		System.out.println("Incorrect operator in the FILTER_WITH section- check query, accepting only =, > , < operations (or) failed to add 'l' or 'd' to notify long or double type with field name");
                			 		return;
                			 }
                			 
                			 System.out.println();
                		
                		} catch(Exception e) {
                			System.out.println("Error converting JSON for FILTER_WITH function - check data, possible data format error (cannot convert to JSON)or empty space in the middle, etc.");
                			return;
                		}
                	}
                	processRow(unmodifiableList(tuple));
                }
            }

            if (nanosSince(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
                flush(false);
            }

            client.advance();
        }
    }

    private void flush(boolean complete)
            throws IOException
    {
        if (!rowBuffer.isEmpty()) {
            printer.printRows(unmodifiableList(rowBuffer), complete);
            rowBuffer.clear();
        }
    }
}
