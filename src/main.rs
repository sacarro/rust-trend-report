extern crate serialize;

use std::collections::BTreeMap;
use std::io::{TcpListener, TcpStream};
use std::io::{Acceptor, Listener};
use std::io::File;
use std::io::IoError;
use std::io::IoResult;
use std::io::Timer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool,Ordering};
use std::thread::Thread;
use std::time::Duration;
use std::os;
use serialize::json::{self, Json,ParserError};
use std::num::FromStrRadix;

struct ProcessedRun<'a> {
	id : &'a str,
	name : &'a str,
	compounds : Vec<Json> 
}

struct ClientRequest {
request_id : usize,
	   www_dir : Path,
	   trend_token : Arc<AtomicBool>
}


impl ClientRequest {


	fn write_error(&self, mut client_stream: TcpStream, code: &str, msg: &str) -> bool {
		let mut data = String::new();
		data.push_str("HTTP/1.1 ");
		data.push_str(code);
		data.push_str("\n");
		data.push_str("Server: Trend Server\n");
		data.push_str("\n");
		data.push_str(msg);
		let result = match client_stream.write_str(data.as_slice()) { Ok(r) => true, Err(e) => false };
		client_stream.flush();

		result
	}

	fn read_line(&self, mut client_stream: TcpStream) -> String {
		let mut request_line : Vec<u8> = vec![];
		loop {
			match client_stream.read_byte() {
				Ok(datum) => {
					match datum as char{
						'\n' => {
							break;
						},
							_ => {
								request_line.push(datum);
							}
					}
				},
					Err(e) => {
						println!("Read Error: {}", e);
						break;
					}
			}
		}

		let mut ret : String;
		if request_line.len() == 0 {
			// return
			ret = String::new();
		} else {
			// return
			ret = String::from_utf8(request_line).unwrap();
		}

		ret
	}

	fn return_file(&self, mut client_stream: TcpStream, path : Path) {

		match File::open(&path).read_to_end() {
			Ok(contents) => {

				let mut data = String::new();
				data.push_str("HTTP/1.1 200 OK\n");
				data.push_str("Server: Trend Server\n");
				data.push_str("Last-Modified: 12 Dec 1983  07:07:07 GMT\n");
				data.push_str("Content-Type: text/html\n");
				data.push_str("Connection: keep-alive\n");
				data.push_str("\n");

				let s = String::from_utf8(contents).unwrap();
				data.push_str(s.as_slice());
				client_stream.write_str(data.as_slice());
				client_stream.flush();
			},
				Err(e) => {
					println!("Writing Error: {}", e);
					self.write_error(client_stream.clone(), "404 Not Found", "Path not found");
				}
		}
	}

	fn write_ok_event_pipe(client_stream : &mut TcpStream) -> bool {
		let mut data = String::new();
		data.push_str("HTTP/1.1 200 OK\n");
		data.push_str("Server: Trend Server\n");
		data.push_str("Last-Modified: 12 Dec 1983  07:07:07 GMT\n");
		data.push_str("Content-Type: text/event-stream\n");
		data.push_str("\n");

		let result = match client_stream.write_str(data.as_slice()) { Ok(r) => true, Err(e) => false };

		result
	}

	fn write_to_event_pipe(mut client_stream : TcpStream, id : usize, event : &str, event_data : &str) -> bool {
		
		let mut data = String::new();
		data.push_str("id:");
		data.push_str(format!("{}", id).as_slice());
		data.push_str("\n");
		data.push_str("event: ");
		data.push_str(event);
		data.push_str("\n");
		data.push_str("data: ");
		data.push_str(event_data);
		data.push_str("\n");
		data.push_str("\n");

		let result = match client_stream.write_str(data.as_slice()) { Ok(r) => true, Err(e) => false };

		result
	}

	fn parse_json_body(&self, mut stream : TcpStream, chunked : bool) -> Result<Json, &str>{
		let mut result : Result<Json, &str>;
		if chunked {
			let mut full_body = String::new();//from_utf8(q.unwrap()).unwrap();
			let mut chunks : Vec<u8> = Vec::new();
			loop {
				let length = self.read_line(stream.clone());
				let n : usize = match FromStrRadix::from_str_radix(length.trim().as_slice(), 16) { Some(n) => n, None => 0 };
				if n == 0 {
					break;
				}
				let q = stream.clone().read_exact(n);
				match q {
					Ok(vals) => {
						chunks.push_all(vals.as_slice());
						//let mut chunk_str : String = match String::from_utf16(vals) { Ok(s) => s, Err(_) => String::new()};
						//full_body.push_str(chunk_str.as_slice());
						//full_body.push_str(String::from_utf8(vals).unwrap().as_slice());
						// Strip off the CRLF
						stream.read_exact(2);
					},
					Err(e) => {
						println!("Could not read chunk: {}", e);
						break;
					}
				}
			}
	

			let mut full_body : String = match String::from_utf8(chunks) { Ok(s) => s, Err(_) => String::new()};
			//full_body = String::from_str("{}");
			//println!("Got this:\n{}\n", full_body);
			match json::from_str(full_body.trim().as_slice()) {
			//match json::from_str("{\"Foo\":\"Bar\"}") {
	        		Ok(json) => result = Ok(json),
		        	Err(err) => {
					match err {
						ParserError::SyntaxError(code, line, col) => println!("Syntax Error: {} {}", line, col),
						ParserError::IoError(e_kind, msg) => println!("IO")
					}
					//println!("JSON is crap: {}", full_body);
					result = Err("Could not parse JSON");//err.error_str;
				}
	        	};
		}else{
			result = Err("Not Yet Implemented!");
		}

		result

	}

	fn get_run_data(&self, query_string : Option<&str>) -> IoResult<Json> {

		let IP_ADDR = "10.211.50.119:80";
		let connect_result = TcpStream::connect(IP_ADDR);
		let mut data : IoResult<Json> = Err(IoError { kind: std::io::IoErrorKind::ConnectionFailed, desc: "Could not get run data", detail: None});
		match connect_result {
			Ok(mut stream) => {

				let mut request : String = String::new();
				match query_string {
					Some(query) => {
						request.push_str("GET /runData?");
						request.push_str(query);
						request.push_str(" HTTP/1.1\n");
					},
					None => request.push_str("GET /runData HTTP/1.1\n")
				};
				request.push_str("Host: 10.211.50.119\n");
				request.push_str("Connection: keep-alive\n");
				request.push_str("Pragma: no-cache\n");
				request.push_str("Accept: */*;q=0.8\n");
				request.push_str("Accept-Language: en-US,en;q=0.8\n");
				request.push_str("\r\n");
				//println!("Requesting: {}", request.as_slice());
				let request_result = stream.write_str(request.as_slice());
				match request_result {
					Ok(_) => {},//println!("Successfully wrote."),
					Err(e) => println!("Couldn't write: {}", e)
				};

				let mut chunked = false;
				loop {
					let line = self.read_line(stream.clone());
					let mut ss = line.split_str(": ");
					let header = match ss.next() { Some(s) => s, None => ""};
					match header.trim().as_slice() {
						"Transfer-Encoding" => chunked = match ss.next() {
							Some(s) => s.trim().as_slice() == "chunked",
								None => false},
						_ => ()
					}

					if line.trim().len() == 0 {
						break;
					}
						
				}

				let runDatum = self.parse_json_body(stream.clone(), chunked);
				match runDatum {
					Ok(json) => {
						data = Ok(json);	
					},
					Err(msg) => println!("Could not get data: {}", msg)
				}

				stream.close_write();
				stream.close_read();
			},
			Err(e) => {
				println!("Could not connect to run data server: {}", e);
			}
		}

		data
	}

	fn find_and_report_compounds(&self, mut client_stream : TcpStream, query_str : &str, mut list : Json){

		// TODO : Need to do something when these aren't here like complete the
		// TODO : trend with some sort of error indication.
		let mut start_time : f64 = -1f64;
		let mut end_time : f64 = -1f64;
		let mut query_string = String::from_str(query_str);
		let mut ss = query_string.split_str("&");
		loop {
			match ss.next() {
				Some(s) => {
					let mut key_value = s.split_str("=");
					let key = match key_value.next() { Some(s) => s, None => ""};
					let value = match key_value.next() { Some(s) => s, None => ""};
					match key {
						"rtStartTime" => {
							start_time = FromStrRadix::from_str_radix(value, 10).unwrap();
						},
						"rtEndTime" => {
							end_time = FromStrRadix::from_str_radix(value, 10).unwrap();
						},
						_ => {}
					}
				},
				None => break
			}
		}

		// Walk through the JSON object to get out a bunch of id's
		let runs : Option<&Json> = match list.find("runs") { Some(t) => Some(t), None => None };
		let mut runs_processed = 0u32;
		match runs {
			Some(runs) => {
				if(runs.is_array()){
					let mut query : String = String::new();
					let mut counter : u8 = 0;
					// FIXME : Rather than all data we can do
					// runTimeStamp, detectors, method
					// because we don't care about the method
					query.push_str("allData=true&id=");
					for run in runs.as_array().unwrap().iter() {
						if(run.is_object()){
							let id = run.as_object().unwrap().get("$id");
							//println!("getting id {}", id.unwrap());
							query.push_str(id.unwrap().as_string().unwrap());
							query.push_str("|");
							counter += 1;
							if counter >= 50 {
								query.pop();
								match self.get_run_data(Some(query.as_slice())) {
									Ok(runs) => {
										self.process_runs(client_stream.clone(), runs, start_time, end_time);
									},
									Err(e) => println!("No runs found: {}", e)
								}
								runs_processed += counter as u32;
								counter = 0;
								query = String::new();
								query.push_str("allData=true&id=");
							}
						}else{
							println!("Throwing away run because not object: {}", run);
						}
					}
				}else{
					// TODO : Return IoResult
					println!("Invalid run response");
				}
			},
			None => println!("No runs to process") 
		}
		println!("Process {} total runs", runs_processed);
		println!("Find and reporting compound trend from {} to {}", start_time, end_time);
	}

	fn process_runs(&self, mut client_stream : TcpStream, mut list : Json, start_time : f64, end_time : f64) -> u32 {

		let runs : Option<&Json> = match list.find("runs") { Some(t) => Some(t), None => None };
		let mut num_of_runs = 0u32;
		match runs {
			Some(runs) => {
				if(runs.is_array()){
					let mut obj_map : json::Object = BTreeMap::with_b(4);
					for j_run in runs.as_array().unwrap().iter() {
						let run = j_run.as_object().unwrap();

						// Setup the tracking object
						obj_map.insert(String::from_str("id"), match run.get("$id") {
							Some(v) => v.clone(),
							None => Json::Null
						});
						obj_map.insert(String::from_str("name"), match run.get("$displayName") {
							Some(v) => v.clone(),
							None => Json::Null
						});
						
						obj_map.insert(String::from_str("timestamp"), match run.get("runTimeStamp") {
							Some(v) => v.clone(),
							None => Json::Null
						});

						// FIXME : For now we only find it in moduleA:tcd
						let peaks : Vec<Json> = match j_run.find_path(&["detectors","moduleA:tcd","analysis","peaks"]){
							Some(j_peaks) => j_peaks.as_array().unwrap().clone(),
							None => Vec::new()
						};

						let mut compound_rt_map : json::Object = BTreeMap::new();
						for peak in peaks.iter() {
							let rt : f64 = match peak.find("top") {
								Some(val) => {
									match val.as_f64() {
										Some(t) => t,
										None => -1f64
									}
								},
								None => -1f64
							};

							if rt != -1f64 {
								let mut compound_details_map : json::Object = BTreeMap::new();
								compound_details_map.insert(String::from_str("name"), match peak.find("label") {
									Some(v) => v.clone(),
									None => Json::Null
								});
								compound_details_map.insert(String::from_str("area"), match peak.find("area") {
									Some(v) => v.clone(),
									None => Json::Null
								});
								compound_details_map.insert(String::from_str("height"), match peak.find("height") {
									Some(v) => v.clone(),
									None => Json::Null
								});
											
	
								// Pop into the map
								compound_rt_map.insert(rt.to_string(), Json::Object(compound_details_map));
							}

						}

						if compound_rt_map.len() > 0 {

							// Write the results to the client stream
							obj_map.insert(String::from_str("compounds"), Json::Object(compound_rt_map));
							let stream_alive = ClientRequest::write_to_event_pipe(client_stream.clone(), self.request_id, "update", format!("{}", Json::Object(obj_map.clone())).as_slice());
							
						}

						// Now clear and reuse the tree
						obj_map.clear();
						num_of_runs += 1;

					}
					println!("Processed {} runs.", num_of_runs);
				}
			},
			None => println!("No runs in JSON")
		};

		// return
		num_of_runs

	}

	fn start_trend(&self, mut client_stream : TcpStream, query_string : &str) {

		let mut invalid_retry_attempt = false;
		loop {
			let header = self.read_line(client_stream.clone());
			match header.as_slice() {
				"\r" => break,
				"\n" => break, // Don't read anything past the headers
				_ => {
					//match header.trim().as_slice() {
					let mut ss = header.split_str(": ");
					let header = match ss.next() { Some(s) => s, None => ""};
					match header.as_slice() {
						"Last-Event-ID" => invalid_retry_attempt = true,
						_ => ()
					}
				}
			}
		}

		client_stream.close_read();

		if invalid_retry_attempt {
			self.write_error(client_stream.clone(), "204 No Content", "Trend has completed.");
		}else{
	
			let mut sleeper = Timer::new().unwrap();

			let mut stream_alive = true;

			// First return a 200 saying we are OK to accept their request
			stream_alive = ClientRequest::write_ok_event_pipe(&client_stream);

			// compare_and_swap returns the original value which if we were able to grab it means
			// that it was false
			loop {
				let has_token = !self.trend_token.compare_and_swap(false, true, Ordering::Relaxed);
				
				if has_token {
					break;
				} else {
					stream_alive = ClientRequest::write_to_event_pipe(client_stream.clone(), self.request_id, "waiting", "Another trend is already running");
					if !stream_alive {
						break;
					}
				}
				// Sleep a bit before retrying
				sleeper.sleep(Duration::seconds(1));
			}

			if stream_alive {	
				stream_alive = ClientRequest::write_to_event_pipe(client_stream.clone(), self.request_id, "started", "Trend Started");
	
				if stream_alive {
					let mut counter : u8 = 0;
					loop {
						match self.get_run_data(None){
							Ok(json) => {
								self.find_and_report_compounds(client_stream.clone(), query_string, json);
							},
							Err(e) => stream_alive = false

						}
						counter += 1;
						if counter >= 1 || !stream_alive {
							break;
						}
						sleeper.sleep(Duration::seconds(1));
					}
		
					stream_alive = ClientRequest::write_to_event_pipe(client_stream.clone(), self.request_id, "complete", "All done");
				}
				println!("did stream die? {}", !stream_alive);
				self.trend_token.compare_and_swap(true, false, Ordering::Relaxed);
			}
		}

		client_stream.close_write();
	}

	fn handle(&self, mut client_stream : TcpStream) {

		println!("Handling request: {}", self.request_id);

		let request_line = self.read_line(client_stream.clone());

		let mut ss = request_line.split_str(" ");
		let request_method = match ss.next() { Some(s) => s, None => ""};
		let request_uri = match ss.next() { Some(s) => s, None => ""};
		ss = request_uri.split_str("?");
		let request_path = match ss.next() { Some(s) => s, None => ""};
		let query_string = match ss.next() { Some(s) => s, None => ""};
		// let http_version = match ss.next() { Some(s) => s, None => ""};

		match (request_method, request_path) {
			("GET", "/") => {
				println!("Request Method: {}", request_method);
				println!("Request URI: {}", request_uri);
				self.return_file(client_stream.clone(), self.www_dir.join("index.html"));
				client_stream.close_read();
				client_stream.close_write();

			},
				("GET", "/trend") => {
					println!("Got trend request");
					self.start_trend(client_stream.clone(), query_string);
				},
				("GET", mut path) => {

					println!("Getting path: {}", path);

					if path.starts_with("/"){
						path = path.slice_from(1);
					}
					self.return_file(client_stream.clone(), self.www_dir.join(path));
					client_stream.close_read();
					client_stream.close_write();
				},
				_ =>{
					println!("Not sure what to do: {}", request_line);
					client_stream.close_read();
					client_stream.close_write();
				}

		}

	}

}

fn main() {

	let www_dir = os::getcwd().unwrap().join("www");
	println!("The current directory is {}", www_dir.display());

	// Start the server.
	let listener = TcpListener::bind("127.0.0.1:8383");

	// bind the listener to the specified address
	let mut acceptor = listener.listen();

	println!("Listening for requests.");
	let mut count : usize = 0;

	let token = Arc::new(AtomicBool::new(false));

	// accept connections and process them, spawning a new tasks for each one
	for stream in acceptor.incoming() {
		let www = www_dir.clone();
		match stream {
			Err(e) => { println!("Incoming stream failure: {}", e); }
			Ok(stream) => {
				count += 1;
				let t : Arc<AtomicBool> = token.clone();
				Thread::spawn(move || {
						let request = ClientRequest{request_id: count, trend_token: t, www_dir: www};
						request.handle(stream);
						});
			}
	
		}
	}

	// close the socket server
	drop(acceptor);


	// Do something else
	println!("Stopping server");

}
