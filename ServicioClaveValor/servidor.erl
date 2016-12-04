%% ----------------------------------------------------------------------------
%% servidor : Modulo principal del servicio de almacen distribuido clave/valor
%%
%% 


%%
%% ----------------------------------------------------------------------------


-module(servidor).

-export([start/3, stop/1]).

-export([init/0, gestionarProcesoLectura/5, gestionarProcesoEscritura/6, gestionarProcesoEscrituraH/6, gestionarBorradoRef/4, actualizarBD/4, actualizador_fun/3]).

-export([sordo/1, escucha/1, fiable/1, no_fiable/1]).
-define(TIMEOUT, 12).
-define(TIEMPO_PROCESADO_PAXOS, 700).

-define(PRINT(Texto,Datos), io:format(Texto,Datos)).
%-define(PRINT(Texto,Datos), ok)).

-define(ENVIO(Mensj, Dest),
        io:format("~p -> ~p -> ~p~n",[node(), Mensj, Dest]), Dest ! Mensj).
%-define(ENVIO(Mensj, Dest), Dest ! Mensj).

-define(ESPERO(Dato), Dato -> io:format("LLega ~p-> ~p~n",[Dato,node()]), ).
%-define(ESPERO(Dato), Dato -> ).


%% Estructura de datos que representa una operación
%  -record(op, {campos...

    % RELLENAR CON CAMPOS que necesitais para representar elementos operación
    
       %  }).


%%%%%%%%%%%%%%%%%%%% FUNCIONES EXPORTABLES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Poner en marcha un nodo Paxos
%% Devuelve :  el nombre completo del nodo erlang.
%% La especificación del interfaz de la función es la siguiente :
%%                      (se puede utilizar también para "dialyzer")
-spec start( [ atom() ], atom(), atom() ) -> node().
start(Servidores, Host, NombreNodo) ->
     % args para comando remoto erl
    Args = "-connect_all false -setcookie palabrasecreta" ++ 
                                            " -pa ./Paxos ./ServicioClaveValor",
    % arranca servidor en nodo remoto
    {ok, Nodo} = slave:start(Host, NombreNodo, Args),
    io:format("Nodo esclavo servidor C/V en marcha ~p~n",[node()]),
    process_flag(trap_exit, true),
    spawn_link(Nodo, ?MODULE, init, []),
    paxos:start(Servidores, Nodo),
    Nodo.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Parar nodo Erlang remoto
-spec stop( atom() ) -> ok.
stop(Nodo) ->
    slave:stop(Nodo),
    timer:sleep(10),
    comun:vaciar_buzon(),
    ok.
    


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pone sordo un nodo Erlang remoto
-spec sordo( atom() ) -> ok.
sordo(Nodo) ->
    {servidor, Nodo} ! ponte_sordo,
    ok.
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pone a escuchar a un nodo Erlang remoto
-spec escucha( atom() ) -> ok.
escucha(Nodo) ->
    {servidor, Nodo} ! escucha,
    ok.
    
   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pone fiable un nodo Erlang remoto
-spec fiable( atom() ) -> ok.
fiable(Nodo) ->
    {servidor, Nodo} ! fiable,
    ok. 

   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pone no fiable un nodo Erlang remoto
-spec no_fiable( atom() ) -> ok.
no_fiable(Nodo) ->
    {servidor, Nodo} ! no_fiable,
    ok. 
%%%%%%%%%%%%%%%%%  FUNCIONES LOCALES  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%-----------------------------------------------------------------------------
init() ->
	register(servidor, self()),
	%lanzar y registra a un proceso que actualice la BD
	PID = spawn_link(node(), ?MODULE, actualizador_fun, 
				[maps:new(), sets:new(), 1]),
	register(actualizador, PID),
	bucle_recepcion(true).

    
%%-----------------------------------------------------------------------------
bucle_recepcion(Fiable) ->
    receive
             
        no_fiable    ->%Edita varaible de fiable = false
		bucle_recepcion(false);
       
        fiable       -> %Edita varaible de fiable = true
		bucle_recepcion(true);

        ponte_sordo -> espero_escucha(Fiable);

        Mensaje_cliente ->
            simula_fallo_mensj_cliente(Mensaje_cliente, Fiable)
    end.
    
%%-----------------------------------------------------------------------------
simula_fallo_mensj_cliente(Mensaje, Es_fiable ) ->
    Aleatorio = rand:uniform(2500),
      %si no fiable, eliminar mensaje con cierta aleatoriedad
    if  ((not Es_fiable) and (Aleatorio < 200)) -> 
                bucle_recepcion(Es_fiable );
                  % Y si lo es tratar el mensaje recibido correctamente
        true -> gestion_mnsj_cliente(Mensaje, Es_fiable )
    end.


%%-----------------------------------------------------------------------------
%% implementar tratamiento de mensajes recibidos en  servidor clave valor
gestion_mnsj_cliente( Mensaje, Es_fiable ) ->

    case Mensaje of

	{leer, Clave, From, Referencia}  -> 
		lectura(Clave, From, Referencia),
		bucle_recepcion( Es_fiable );

	{escribir, Clave, Valor, From, Referencia} -> 
		escritura(Clave, Valor, From, Referencia),
		bucle_recepcion(Es_fiable );

	{escribir_hash, Clave, Valor, From, Referencia} -> 
		escritura_hash(Clave, Valor, From, Referencia),
		bucle_recepcion(Es_fiable );

	{borrar_ref, Referencia, From} ->
		borrar_referencia(Referencia, From),
		bucle_recepcion(Es_fiable );

	_ ->	?PRINT("Recibido mensaje Servidor no identificado: ~p~n",[Mensaje]), 
			bucle_recepcion(Es_fiable )

	end.


%% Se realiza una lectura
lectura(Clave, From, Referencia)->
	NumInstancia = paxos:max(node())+1,
	% Se intenta proponer la instancia
	Respuesta = paxos:start_instancia(node(), NumInstancia, {leer, Clave, Referencia}),

	case Respuesta of
		ok -> From ! ok,
		% responder que se ha iniciado paxos y que después se le enviará el valor si se llega a consenso
		spawn_link(node(), ?MODULE, gestionarProcesoLectura, 
				[NumInstancia, From, Clave, 50, Referencia]);
		ya_existe_proponente -> self() ! {leer, Clave, From, Referencia};%reintentarlo
		_Otra_respuesta -> From ! caido % Paxos esta caido
	end.


%% Se realiza una escritura
escritura(Clave, Valor, From, Referencia)->
	NumInstancia = paxos:max(node())+1,
	% Se intenta proponer la instancia
	Respuesta = paxos:start_instancia(node(), NumInstancia, {escribir, Clave, Valor, Referencia}),

	case Respuesta of
		ok -> From ! ok,
		% responder que se ha iniciado paxos y que después se le enviará el valor si se llega a consenso
		spawn_link(node(), ?MODULE, gestionarProcesoEscritura, 
				[NumInstancia, From, Clave, Valor, 50, Referencia]);
		ya_existe_proponente -> self() ! {escribir, Clave, Valor, From, Referencia};%reintentarlo
		_Otra_respuesta -> From ! caido % Paxos esta caido
	end.


%% Se realiza una escritura hash
escritura_hash(Clave, Valor, From, Referencia)->
	NumInstancia = paxos:max(node())+1,
	% Se intenta proponer la instancia
	Respuesta = paxos:start_instancia(node(), NumInstancia, {escribir_hash, Clave, Valor, Referencia}),

	case Respuesta of
		ok -> From ! ok,
		% responder que se ha iniciado paxos y que después se le enviará el valor si se llega a consenso
		spawn_link(node(), ?MODULE, gestionarProcesoEscrituraH, 
				[NumInstancia, From, Clave, Valor, 50, Referencia]);
		ya_existe_proponente -> self() ! {escribir_hash, Clave, Valor, From, Referencia};%reintentarlo
		_Otra_respuesta -> From ! caido % Paxos esta caido
	end.


%% Se borra una referencia
borrar_referencia(Referencia, From)->
	NumInstancia = paxos:max(node())+1,
	% Se intenta proponer la instancia
	Respuesta = paxos:start_instancia(node(), NumInstancia, {borrar_ref, Referencia}),

	case Respuesta of
		ok ->  From ! ok,	
		% responder que se ha iniciado paxos y que después se le enviará el valor si se llega a consenso
		spawn_link(node(), ?MODULE, gestionarBorradoRef, [NumInstancia,  50, Referencia, From]);
		_Otra_respuesta -> self() ! {borrar_ref, Referencia, From}%reintentarlo
	end.



%%Gestiona la lectura a partir de que la instancia se ha llevado a consenso
gestionarProcesoLectura(NumInstancia, Cliente,  Clave, TiempoEspera, Referencia)-> 
	if TiempoEspera < (?TIEMPO_PROCESADO_PAXOS)/3 ->
		timer:sleep(TiempoEspera),
		Estado = paxos:estado(node(), NumInstancia),
		?PRINT("Servidor (GL): ~p, Estado: ~p ~n",[node(), Estado]),
		case Estado of
		{true, {leer, Clave, _Referencia}} ->%Hay un valor, devovler valor
			actualizar_bd_lectura(NumInstancia, Clave, Cliente);
		{true, _Otra_instancia}  ->
			%Hay un valor, pero no es el de tu lectura , vovler a proponer la instancia
			{servidor, node()} ! {leer, Clave, Cliente, Referencia};
		{false, _Valor} -> %aun no hay valor decidido, dormirte y volver a preguntar
			gestionarProcesoLectura(NumInstancia, Cliente,
					 Clave, TiempoEspera+20, Referencia)
		end;
	true-> morir
	end.



%% Actualiza una lectura en la bD y responde al cliente con el resultado correspondiente
actualizar_bd_lectura(NumInstancia, Clave, Cliente)->
	%El servidor actualizar su BD respecto de las operaciones de paxos hasta la del cliente
	{actualizador, node()} ! {actualizar_bd, NumInstancia, self()},
	receive
	{actualizado, BD} -> %Devuelves el valor de la lectura
		Lectura = maps:find(Clave, BD),
		case Lectura of
			{ok, Value} ->  Cliente ! {respuesta_lectura, Value} ;
			error ->  Cliente ! {respuesta_lectura, ""} 
		end
	end.


%% Gestiona la escritura a partir de que la instancia se ha llevado a consenso
gestionarProcesoEscritura(NumInstancia, Cliente,  Clave, Valor, TiempoEspera, Referencia)-> 
	if TiempoEspera < (?TIEMPO_PROCESADO_PAXOS)/3 ->
		timer:sleep(TiempoEspera),
		Estado = paxos:estado(node(), NumInstancia),
		?PRINT("Servidor (GE): ~p, Estado: ~p ~n",[node(), Estado]),
		case Estado of
		{true, {escribir, Clave, Valor, Referencia}} ->%Hay un valor, devovler valor
			%Enviar ya la respuesta al cliente
			Cliente ! {respuesta_escritura, ""};% No hace falta actualizar
		{true, _Otra_instancia}  ->
			%Hay un valor, pero no es el de tu lectura , vovler a proponer la instancia
			{servidor, node()} ! {escribir, Clave, Valor, Cliente, Referencia};
		{false, _Valor} -> %aun no hay valor decidido, dormirte y volver a preguntar
			gestionarProcesoEscritura(NumInstancia,  Cliente, 
					 Clave, Valor, TiempoEspera+50, Referencia)
		end;
	true-> morir

	end.


    
%% Gestiona la escritura_hash a partir de que la instancia se ha llevado a consenso
gestionarProcesoEscrituraH(NumInstancia, Cliente,  Clave, Valor, TiempoEspera, Referencia)-> 
	if TiempoEspera < (?TIEMPO_PROCESADO_PAXOS)/3 ->
		timer:sleep(TiempoEspera),
		Estado = paxos:estado(node(), NumInstancia),
		?PRINT("Servidor (GEh): ~p, Estado: ~p ~n",[node(), Estado]),
		case Estado of
		{true, {escribir_hash, Clave, _Valor, Referencia}} ->
			actualizar_bd_escrituraHash(NumInstancia, Clave, Cliente);
		{true, _Otra_instancia}  ->
			%Hay un valor, pero no es el de tu lectura , vovler a proponer la instancia
			{servidor, node()} ! {escribir_hash, Clave, Valor, Cliente, Referencia};
		{false, _Valor} -> %aun no hay valor decidido, dormirte y volver a preguntar
			gestionarProcesoEscrituraH(NumInstancia,  Cliente,  Clave, 
							Valor,TiempoEspera+50, Referencia)
		end;
	true-> morir
	end.



%% Actualiza una escritura en la bD y responde al cliente con el resultado correspondiente
actualizar_bd_escrituraHash(NumInstancia, Clave, Cliente)->
	% Actualizas hasta una operacion antes de la escritura (para leer el valor anterior al hash
	{actualizador, node()} ! {actualizar_bd, NumInstancia-1, self()},
	receive
	{actualizado, BD} -> %Devuelves el valor de la lectura
		Lectura = maps:find(Clave, BD),
		case Lectura of
			{ok, Value} ->  Cliente ! {respuesta_escritura, Value};
			error ->  Cliente ! {respuesta_escritura, ""} 
		end
	end.



%% Gestiona el borrado de una referencia la cual ya no es necesaria guardarla ya
%% a partir de la instancia que se ha llevado a consenso
gestionarBorradoRef(NumInstancia, TiempoEspera, Referencia, Cliente)-> 
		timer:sleep(TiempoEspera),
		Estado = paxos:estado(node(), NumInstancia),
		case Estado of
		{true, {borrar_ref, Referencia}} ->
			borado_consistente;

		{true, _Otra_instancia} ->
			%Hay un valor, pero no es el de tu lectura , vovler a proponer la instancia
			{servidor, node()} ! {borrar_ref, Referencia, Cliente};

		{false, _Valor} -> %aun no hay valor decidido, dormirte y volver a preguntar
			gestionarBorradoRef(NumInstancia, TiempoEspera+50, Referencia, Cliente)
		end.



%% Dada una operacion a escritura a actualizar, la actualiza respecto a la BD y las referencias
actualizar_escritura_hash(Clave, Valor, Referencia, Desde, Hasta, BD, Ref_ops)->
	Es_elemento = sets:is_element(Referencia, Ref_ops),
	if not Es_elemento  ->% Si es una operacion repetida
		Lectura = maps:find(Clave, BD),
		case Lectura of
			{ok, Value} ->  Anterior = Value ;
			error ->  Anterior = "" 
		end,
		NewBD = maps:put(Clave, comun:hash(Anterior ++ Valor), BD),
		Ref_ops_new = sets:add_element(Referencia, Ref_ops),
		Respuesta = actualizarBD(NewBD, Ref_ops_new, Desde + 1, Hasta);
	true-> Respuesta = actualizarBD(BD, Ref_ops, Desde + 1, Hasta)
	end,
	Respuesta.



%% Dada una operacion escritura hash a actualizar, la actualiza respecto a la BD y las referencias
actualizar_escritura(Clave, Valor, Referencia, Desde, Hasta, BD, Ref_ops)->
	Es_elemento = sets:is_element(Referencia, Ref_ops),
	if not Es_elemento  ->% Si es una operacion repetida
		NewBD = maps:put(Clave, Valor, BD),
		Ref_ops_new = sets:add_element(Referencia, Ref_ops),
		Respuesta = actualizarBD(NewBD, Ref_ops_new, Desde + 1, Hasta);
	true-> Respuesta = actualizarBD(BD, Ref_ops, Desde + 1, Hasta)
	end,
	Respuesta.



%% Dada una operacion a actualizar, la actualiza respecto a la BD y las referencias
actualizar_operacion(Estado, BD, Ref_ops, Desde, Hasta)->
	case Estado of 
		{true, {leer, _Clave, Referencia}} ->%Las lecturas no afectan a la BD
			Ref_ops_new = sets:del_element(Referencia, Ref_ops),
			Respuesta = actualizarBD(BD, Ref_ops_new , Desde + 1, Hasta);

		{true, {borrar_ref, _Referencia}} ->%Las borrados de refs no afectan a la BD
			Respuesta = actualizarBD(BD, Ref_ops, Desde + 1, Hasta);

		{true, {escribir, Clave, Valor, Referencia}} ->
			Respuesta = actualizar_escritura(Clave, Valor, Referencia, Desde, Hasta, BD, Ref_ops);

		{true, {escribir_hash, Clave, Valor, Referencia}} ->
			Respuesta = actualizar_escritura_hash(Clave, Valor, Referencia, Desde, Hasta, BD, Ref_ops);

		{false, _Valor} -> 	
			% Si no hay valor decidido aun (porque eta caido paxos 
			% o porque aun no se ha decidido) seguir probandolo
			Respuesta = actualizarBD(BD, Ref_ops, Desde, Hasta)
		end,
		Respuesta.


%% Actualiza la BD y las Referencias dado un intervalo de instancias ya registradas en Paxos (consistentes)
actualizarBD(BD, Ref_ops, Desde, Hasta)->
	%Intentar actualizar el valor en BD
	if Hasta >= Desde -> 
		Estado = paxos:estado(node(), Desde),
		Respuesta = actualizar_operacion(Estado, BD, Ref_ops, Desde, Hasta);
	true-> Respuesta = {BD, Ref_ops} % no hacer nada mas
	end,
	Aleatorio = rand:uniform(2500), %Se libera memoria un 20% de las veces para no sobrecargar
	if  (Aleatorio < 200) -> 
		paxos:hecho(node(), Hasta);
		  % Y si lo es tratar el mensaje recibido correctamente
	true -> no_liberas
	end,
	Respuesta.




%% Recibe peticiones de actualización de la BD
actualizador_fun(BD, Ref_ops, InstanciaMaxActual)->
	receive
		{actualizar_bd, Hasta, From} ->
			{NuevaBD, NuevoRefs} = actualizarBD(BD, Ref_ops, InstanciaMaxActual, Hasta),
			From ! {actualizado, NuevaBD},
			?PRINT("Servidor: ~p, BD actualizada ~n",[node()]),
			actualizador_fun(NuevaBD, NuevoRefs, Hasta + 1)
	end.



%% Estado sordo hasta que recibe un escucha
espero_escucha(Fiable) ->
    io:format("~p : Esperando a recibir escucha~n",[node()]),
    receive
        escucha ->
            io:format("~p : Salgo de la sordera !!~n",[node()]),
            bucle_recepcion(Fiable);
        Resto -> io:format("ESTOYSORDO ~p: ~p ~n",[Resto, node()]),espero_escucha(Fiable)
    end.

