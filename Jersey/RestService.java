/**
 *  Copyright 2014 Romil Bansal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package Jersey;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import version1.Disambiguator;

import com.sun.jersey.multipart.FormDataParam;

@Path("")
public class RestService {

	Disambiguator ds = new Disambiguator();
	private static boolean debug = false;
	
	/*respods in form request*/
	@POST
	@Path("/shortTrack")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces({ MediaType.TEXT_PLAIN })
	public String annotatePost(@FormDataParam("runID") String runId,
			@FormDataParam("TextID") String textId,
			@FormDataParam("Text") String text) {
		String output = "";
		try {
			ds.bw.append(runId +" "+textId+" "+text+"->\n");
			if(debug){				
				return "";
			}
			ds.bw.flush();
			output = ds.getResultsFormatted(textId, text);
			ds.bw.append(output.trim());
			ds.bw.newLine();			
			ds.bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(output);
		return output;
	}

	/*respods in browser request*/
	@GET
	@Path("/shortTrack")
	@Produces({ MediaType.TEXT_PLAIN })
	public String annotateGet(@QueryParam("runID") String runId,
			@QueryParam("TextID") String textId, @QueryParam("Text") String text) {
		//System.out.println(runId +" "+textId+" "+text);
		String output = "";
		//System.out.println(output);
		try {
			ds.bw.append(runId +" "+textId+" "+text);
			output = ds.getResultsFormatted(textId, text);
			System.out.println(output);		
			ds.bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output;
	}

}
