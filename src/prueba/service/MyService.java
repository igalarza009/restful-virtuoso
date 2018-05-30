package prueba.service;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;


@Path("/service")
public class MyService {
	
	private String url = "jdbc:virtuoso://localhost:1111";

	@Path("/hello")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String hello(@FormParam("name") String name) {
	 	return "Hello " + name;
	}
	   
	@Path("/query")
//	@GET
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response getSPARQLQueryResult_JSON(@FormParam("query") String consulta){ //	@QueryParam
	 	String json=null;
	 	VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	System.out.println("Conexión establecida");
	 	Query query = QueryFactory.create(consulta);
	 	VirtuosoQueryExecution qe = VirtuosoQueryExecutionFactory.create(query, set);
	 	System.out.println("Ejecutamos la consulta");
	 	try {
	 		ResultSet results = qe.execSelect() ;
	 	    // write to a ByteArrayOutputStream
	 	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	 	    ResultSetFormatter.outputAsJSON(outputStream, results);
	 	    // and turn that into a String
	 	    json = new String(outputStream.toByteArray());
	 	    System.out.println("Guardamos los resultados en variable json");
	 	 } catch (Exception e) {
	 	    e.printStackTrace();
	 	 } finally {
	 	    qe.close();
	 	 }
	 	 // return json;
	 	System.out.println("Devolvemos la respuesta");
	 	 return Response.ok(json).build();
	   }
	   
	   @Path("/insert")
//	   @GET
	   @POST
	   @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	   @Produces("application/json")//Mirar cómo quitar o modificar esto  
	   public Response insertRDF(@FormParam("query") String query){	//@QueryParam    
	 	    //String query = "INSERT DATA {graph <prueba01> {<#book7> <#price> 47 .}}";	    
	 	    VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	    VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, set);
	 	    vur.exec(); 
	 	    return Response.ok().build();
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

	 	    return Response.ok().build();
	   }
	   
	   @Path("/insertfile")
	   @POST
//	   @GET
	   @Consumes(MediaType.MULTIPART_FORM_DATA)
	   @Produces("application/json")//Mirar cómo quitar o modificar esto   
	   public Response bulkInsertRDF_2(
			   				@FormDataParam("file") InputStream uploadedInputStream,
			   				@FormDataParam("file") FormDataContentDisposition fileDetail){
//		   System.out.println("Hola!!");
		   try
		   {
		       String line;
		       BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(uploadedInputStream));
		       while( (line = bufferedReader.readLine()) != null )
		       { 
		           System.out.printf("%s\n", line);
		       }  
		   } 
		   catch( IOException e )
		   {
		       System.err.println( "Error: " + e );
		   }

	 	    return Response.ok().build();
	   }
	
	
}
