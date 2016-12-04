erl -noshell -name maestro@127.0.0.1 -rsh ssh -setcookie 'palabrasecreta' -eval "eunit:test(paxos)."  -run init stop


erl -rsh h -name maestro@127.0.0.1  -setcookie 'palabrasecreta'
c(datos_paxos).
c(paxos_tests).
c(aceptador).
c(proponente).
c(paxos).

paxos_tests:no_hay_decision_si_particionado().

qutiar el puto sino_matoria reinicia



%Supuestos pasado

paxos_tests:unico_proponente().
paxos_tests:varios_propo_un_valor().
paxos_tests:varios_propo_varios_valores().
paxos_tests:instancias_fuera_orden().
paxos_tests:proponentes_sordos().
paxos_tests:muchas_instancias().
paxos_tests:muchas_instancias_no_fiable().
paxos_tests:decision_en_particion_mayoritaria().
paxos_tests:olvidando().
