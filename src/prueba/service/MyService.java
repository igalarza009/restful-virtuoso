package prueba.service;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

@Path("/service")
public class MyService {
	
	private String url = "jdbc:virtuoso://localhost:1111";

	@Path("/hello")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String hello() {
		return "Hello World()";
	}
	   
	@Path("/query")
	@GET
	@Produces("application/json")
	public Response getSPARQLQueryResult_JSON(@QueryParam("query") String consulta){
		System.out.println("Hola");
	 	String json=null;
	 	VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	//consulta = "SELECT * from <prueba01> WHERE {?s <#price> ?o}";
	 	Query query = QueryFactory.create(consulta);
	 	VirtuosoQueryExecution qe = VirtuosoQueryExecutionFactory.create(query, set);
	 	try {
	 		ResultSet results = qe.execSelect() ;
	 	    // write to a ByteArrayOutputStream
	 	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	 	    ResultSetFormatter.outputAsJSON(outputStream, results);
	 	    // and turn that into a String
	 	     json = new String(outputStream.toByteArray());	       
	 	 } catch (Exception e) {
	 	    e.printStackTrace();
	 	 } finally {
	 	    qe.close();
	 	 }
	 	 // return json;
	 	 return Response.ok(json).header("Access-Control-Allow-Origin", "*").build();
	   }
	   
	   @Path("/insert")
	   @GET
	   @Produces("application/json")//Mirar cómo quitar o modificar esto  
	   public Response insertRDF(@QueryParam("query") String query){	    
	 	    //String query = "INSERT DATA {graph <prueba01> {<#book7> <#price> 47 .}}";	    
	 	    VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	    VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, set);
	 	    vur.exec(); 
	 	    return Response.ok().header("Access-Control-Allow-Origin", "*").build();
	   }
	   
	   @Path("/bulkinsert")
	   @GET
	   @Produces("application/json")//Mirar cómo quitar o modificar esto   
	   public Response bulkInsertRDF(@QueryParam("file") String file, @QueryParam("graph") String graph) throws FileNotFoundException{	    
	    	    String query="", linea;
	 	    VirtuosoUpdateRequest vur;
	     
	 	    VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	    
	 	    @SuppressWarnings("resource")
	 		Scanner input = new Scanner(new FileReader("data/"+file));
	 	    while(input.hasNext()){
	 	    	linea=input.nextLine();	    	
	 	    	query = "INSERT DATA ";
	 	    	if(graph!=null){
	 	    		query = query + "{graph <"+graph+"> ";	    	
	 	    	}
	 	    	query = query + "{"+linea+"}";
	 	    	if(graph!=null){
	 	    		query = query+"}";	    	
	 	    	}
	 	    	System.out.println(query);
	 	    	vur= VirtuosoUpdateFactory.create(query, set);
	 	    	vur.exec();	    	
	 	    }

	 	    return Response.ok().header("Access-Control-Allow-Origin", "*").build();
	   }
	
}
