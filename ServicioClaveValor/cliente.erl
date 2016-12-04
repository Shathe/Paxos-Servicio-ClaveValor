%% ----------------------------------------------------------------------------
%% cliente : API cliente del servicio de almacen distribuido clave/valor
%%
%% 


%%
%% ----------------------------------------------------------------------------


-module(cliente).

-export([start/3, stop/1, lee/2,  escribe/3, escribe_hash/3]).

-export([init/1]).

-define(TIMEOUT, 200).

-define(TIEMPO_PROCESADO_PAXOS, 700).

-define(PRINT(Texto,Datos), io:format(Texto,Datos)).
%-define(PRINT(Texto,Datos), ok)).

-define(ENVIO(Mensj, Dest),
        io:format("~p -> ~p -> ~p~n",[node(), Mensj, Dest]), Dest ! Mensj).
%-define(ENVIO(Mensj, Dest), Dest ! Mensj).

-define(ESPERO(Dato), Dato -> io:format("LLega ~p-> ~p~n",[Dato,node()]), ).
%-define(ESPERO(Dato), Dato -> ).



%%%%%%%%%%%% FUNCIONES EXPORTABLES


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  Poner en marcha un nodo cliente
%%  Devuelve :  el nombre completo del nodo erlang.
%% La especificación del interfaz de la función es la siguiente :
%%                      (se puede utilizar también para "dialyzer")
-spec start( [ atom() ], atom(), atom() ) -> node().
start(Servidores, Host, NombreNodo) ->
     % args para comando remoto erl
    Args = "-connect_all false -setcookie palabrasecreta" ++ 
                                            " -pa ./Paxos ./ServicioClaveValor",

        % arranca cliente clave/valor en nodo remoto
    {ok, Nodo} = slave:start(Host, NombreNodo, Args),
    io:format("Nodo cliente en marcha ~p ~n",[node()]),
    process_flag(trap_exit, true),
    spawn_link(Nodo, ?MODULE, init, [Servidores]),
    Nodo.
    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Parar nodo Erlang remoto
-spec stop(atom()) -> ok.
stop(Nodo) ->
    slave:stop(Nodo),
    timer:sleep(10),
    comun:vaciar_buzon(),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Obtener el valor en curso de la clave
%% - Devuelve cadena vacia de caracteres ("") si no existe la clave
%% - Seguir intentandolo  en el resto de situaciones de fallo o error
%% La especificación del interfaz de la función es la siguiente :
%%                      (se puede utilizar también para "dialyzer")					
-spec lee( node(), string() ) -> string().
lee(NodoCliente, Clave) ->
	lee(NodoCliente, Clave, make_ref()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Obtener el valor en curso de la clave
%% - Devuelve cadena vacia de caracteres ("") si no existe la clave
%% - Seguir intentandolo  en el resto de situaciones de fallo o error
%% La especificación del interfaz de la función es la siguiente :
%%                      (se puede utilizar también para "dialyzer")					
lee(NodoCliente, Clave, Referencia) ->
	%Obtener servidores 	
	{cliente, NodoCliente} ! {get_servidores, self()},
	receive {servidores_cliente, Servidores} -> 
		Respuesta = hacer_peticion_lectura(NodoCliente, Clave, Referencia, Servidores)
	after 30 -> ?PRINT("Lectura Cliente ~p ~p, falla Servidores , reintento~n",
			[NodoCliente, Clave]), Respuesta = nodo_cliente_no_disponible
	end,
	Respuesta.

%% Realiza las peticiones correspondientes al servidor siempre consiguiendo una respuesta
hacer_peticion_lectura(NodoCliente, Clave, Referencia, Servidores) ->
	Server=siguienteServidor(Servidores),
	?PRINT("Se obtiene servidor:~p en Cliente ~p ~p~n",[Server, NodoCliente, Clave]),
	%Por cada nodo, llamar a un servidor vivo
	{servidor, Server} ! {leer, Clave, self(), Referencia}, 
	% Encontrar un Servidor vivo
	receive 
		ok -> 	Respuesta = lectura_from_Servidor(NodoCliente, Clave, Referencia, Server);

		caido ->  ?PRINT("Lectura Cliente ~p ~p,  servidor caido , reintento~n",
			  [NodoCliente, Clave]), 
			  Respuesta = lee(NodoCliente, Clave, Referencia)

	after ?TIMEOUT ->?PRINT("Lectura Cliente ~p ~p, falla respuesta servidor concreto  ~p, reintento~n",
			[NodoCliente, Clave, Server]), 
			Respuesta = lee(NodoCliente, Clave, Referencia)
	end,
	Respuesta.


%% Una vez se tiene un servidor que se ha comprometido a responder, se espera su respuesta
lectura_from_Servidor(NodoCliente, Clave, Referencia, Server) ->
	Tiempo_Procesado = rand:uniform(200) + ?TIEMPO_PROCESADO_PAXOS,
	?PRINT("El servidor ha iniciado tu petición a paxos: Cliente ~p ~p~n",[NodoCliente, Clave]),
	%El servidor ha iniciado tu petición a paxos, esperar la respuesta de la lectura
	receive {respuesta_lectura, Respuesta} -> 
			?PRINT("LecturaRESULTADO Cliente ~p ~p: ~p~n", [NodoCliente, Clave, Respuesta]),
			{servidor, Server} ! {borrar_ref, Referencia, self()},
			Respuesta
	after Tiempo_Procesado ->
			?PRINT("Lectura Cliente ~p ~p, falla procesarPaxos-devolver , reintento~n",
			[NodoCliente, Clave]), 
			Respuesta = lee(NodoCliente, Clave, Referencia)
	end,
	Respuesta.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Escribir un valor para una clave
%% - Seguir intentandolo hasta que se tenga exito
%% - Devuelve valor anterior si hash y nuevo sino
%% La especificación del interfaz de la función es la siguiente :
%%                      (se puede utilizar también para "dialyzer")
escribe_generico(NodoCliente, Clave, Valor, ConHash, Referencia) ->
	%Obtener servidores 
	{cliente, NodoCliente} ! {get_servidores, self()},
	receive {servidores_cliente, Servidores} -> 
		Respuesta = hacer_peticion_escritura(NodoCliente, Clave, Valor,
							ConHash, Referencia, Servidores)

	after 30 -> ?PRINT("Escritura Cliente ~p ~p, falla Servidores , reintento~n", [NodoCliente, Clave]), 
		    Respuesta = nodo_cliente_no_disponible
	end,
	
	Respuesta.



%% Realiza las peticiones correspondientes al servidor siempre consiguiendo una respuesta
hacer_peticion_escritura(NodoCliente, Clave, Valor, ConHash, Referencia, Servidores) ->
	Server=siguienteServidor(Servidores),
	?PRINT("Se obtiene servidor:~p en Cliente ~p ~p~n",[Server, NodoCliente, Clave]),
	%Por cada nodo, llamar a un servidor vivoy y  Encontrar un Servidor vivo
	case ConHash of
		false -> {servidor, Server} ! {escribir, Clave, Valor, self(), Referencia};
		true  -> {servidor, Server} ! {escribir_hash, Clave, Valor, self(), Referencia}
	end,
	receive 
		ok -> 	Respuesta = escritura_from_Servidor(NodoCliente, Clave, Valor, 
								ConHash, Referencia, Server);
		caido ->  ?PRINT("Escritura Cliente ~p ~p,  servidor caido , reintento~n", [NodoCliente, Clave]),
			  Respuesta = escribe_generico(NodoCliente, Clave, Valor, ConHash, Referencia)
	after ?TIMEOUT ->?PRINT("Escritura Cliente ~p ~p, falla respuesta servidor concreto ~p , reintento~n",
											[NodoCliente, Clave, Server]),
		Respuesta = escribe_generico(NodoCliente, Clave, Valor, ConHash, Referencia)
	end,
	Respuesta.


%% Una vez se tiene un servidor que se ha comprometido a responder, se espera su respuesta
escritura_from_Servidor(NodoCliente, Clave, Valor, ConHash, Referencia, Server) ->
	Tiempo_Procesado = rand:uniform(200) + ?TIEMPO_PROCESADO_PAXOS,
	?PRINT("El servidor ha iniciado tu petición a paxos: Cliente ~p ~p ~p~n",[NodoCliente, Clave, Valor]),
	%El servidor ha iniciado tu petición a paxos, esperar la respuesta de la lectura
	receive {respuesta_escritura, Respuesta} -> 
		?PRINT("EscrituraRESULTADO Cliente ~p ~p: ~p~n",[NodoCliente, Clave, Respuesta]),
		{servidor, Server} ! {borrar_ref, Referencia, self()},
	Respuesta

	after Tiempo_Procesado ->
		?PRINT("Escritura Cliente ~p ~p, falla procesarPaxos-devolver , reintento~n",[NodoCliente, Clave]),
		Respuesta = escribe_generico(NodoCliente, Clave, Valor, ConHash, Referencia)
	end,
	Respuesta.
	



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - Devuelve nuevo valor escrito
-spec escribe( node(), string(), string() ) -> string().
escribe(NodoCliente, Clave, Valor) ->
    escribe_generico(NodoCliente, Clave, Valor, false, make_ref()).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - Devuelve valor anterior
-spec escribe_hash( node(), string(), string() ) -> string().
escribe_hash(NodoCliente, Clave, Valor) ->
    escribe_generico(NodoCliente, Clave, Valor, true, make_ref()).
    
    
%%%%%%%%%%%%%%%%%%%%%%%%%%  FUNCIONES LOCALES


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Servidores) ->
    register(cliente, self()),
    bucle_recepcion(Servidores).
    
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
bucle_recepcion(Servidores) ->

	receive
	% Devuelve los servidores disponibles
	{get_servidores, From} ->?PRINT("enviando servidores Cliente: ~p~n",[Servidores]),
	    		From ! {servidores_cliente, Servidores},
			ServidoresShift=shiftServidores(Servidores),
			bucle_recepcion(ServidoresShift);

	 Mensaje ->	?PRINT("Recibido mensaje cliente no identificado: ~p~n",[Mensaje])
			
	end.
	

    
%% Devuelve el primer elemento de una lista
siguienteServidor([S|_Servidores])->S.

%% El primer elemento de la lista pasa a ser el último
shiftServidores([S|Servidores])-> Servidores ++ [S].

 
